mod engine;
mod kernel;
mod vsr;
mod chaos;

use std::env;
use std::fs;
use std::path::Path;
use std::process;

use engine::format::GENESIS_HASH;
use engine::log::LogWriter;
use engine::recovery::{LogRecovery, RecoveryOutcome};
use tracing::info;
use tracing_subscriber::EnvFilter;

const LOG_PATH: &str = "/tmp/chr_crash_test.log";
const ENTRIES_PER_RUN: u64 = 100;

fn main() {
    // Initialize structured logging
    // Use RUST_LOG env var to control log level (e.g., RUST_LOG=debug)
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("chr=info".parse().unwrap()))
        .with_target(true)
        .with_thread_ids(true)
        .init();
    
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        match args[1].as_str() {
            "clean" => {
                // Clean start: remove existing log and start fresh
                let _ = fs::remove_file(LOG_PATH);
                println!("Cleaned log file.");
                return;
            }
            "write" => {
                // Write mode: append entries, possibly crash
                run_write_phase();
                return;
            }
            "recover" => {
                // Recovery mode: run recovery and report state
                run_recovery_phase();
                return;
            }
            "test" => {
                // Full test: clean, write with crash, recover
                run_crash_test();
                return;
            }
            _ => {
                print_usage();
                return;
            }
        }
    }

    // Default: run the crash test
    run_crash_test();
}

fn print_usage() {
    eprintln!("Usage: chr [command]");
    eprintln!("Commands:");
    eprintln!("  clean   - Remove existing log file");
    eprintln!("  write   - Append entries (may crash randomly)");
    eprintln!("  recover - Run recovery and report state");
    eprintln!("  test    - Full crash test cycle");
    eprintln!("  (none)  - Same as 'test'");
}

/// Run the write phase: append entries, possibly crash mid-write.
fn run_write_phase() {
    let path = Path::new(LOG_PATH);

    // First, recover to get current state
    let (next_index, write_offset, tail_hash, view_id) = match LogRecovery::open(path) {
        Ok(Some(recovery)) => {
            match recovery.scan() {
                Ok(outcome) => match outcome {
                    RecoveryOutcome::CleanEmpty { .. } => (0, 0, GENESIS_HASH, 1),
                    RecoveryOutcome::Clean {
                        last_index,
                        next_offset,
                        tail_hash,
                        highest_view,
                        ..
                    } => (last_index + 1, next_offset, tail_hash, highest_view),
                    RecoveryOutcome::Truncated {
                        last_valid_index,
                        new_offset,
                        tail_hash,
                        highest_view,
                        ..
                    } => {
                        if last_valid_index == 0 && new_offset == 0 {
                            (0, 0, GENESIS_HASH, 1)
                        } else {
                            (last_valid_index + 1, new_offset, tail_hash, highest_view)
                        }
                    }
                },
                Err(e) => {
                    eprintln!("FATAL: Recovery failed: {}", e);
                    process::exit(1);
                }
            }
        }
        Ok(None) => (0, 0, GENESIS_HASH, 1),
        Err(e) => {
            eprintln!("FATAL: Failed to open log: {}", e);
            process::exit(1);
        }
    };

    println!(
        "Starting write phase: next_index={}, offset={}",
        next_index, write_offset
    );

    let mut writer = match LogWriter::open(path, next_index, write_offset, tail_hash, view_id) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("FATAL: Failed to open log writer: {}", e);
            process::exit(1);
        }
    };

    // Determine crash point using simple deterministic "randomness"
    // In a real test, you'd use actual randomness or external injection
    let crash_after = get_crash_point(next_index);

    for i in 0..ENTRIES_PER_RUN {
        let payload = format!("entry-{}-{}", next_index + i, std::process::id());

        // Simulate crash before write completes
        if i == crash_after {
            println!("SIMULATING CRASH at entry {} (before fdatasync)", next_index + i);
            // Exit without completing the write - simulates crash before durability
            process::exit(0);
        }

        // Use current wall clock as timestamp for demo purposes
        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        match writer.append(payload.as_bytes(), 0, 0, timestamp_ns) {
            Ok(idx) => {
                if i % 10 == 0 {
                    println!("Wrote entry {}", idx);
                }
            }
            Err(e) => {
                eprintln!("FATAL: Write failed at index {}: {}", next_index + i, e);
                process::exit(1);
            }
        }
    }

    println!(
        "Write phase complete. Last index: {}",
        writer.next_index() - 1
    );
}

/// Get a deterministic "random" crash point based on current state.
fn get_crash_point(next_index: u64) -> u64 {
    // Use process ID and index to vary crash points across runs
    let pid = std::process::id() as u64;
    let seed = next_index.wrapping_add(pid);
    let hash = seed.wrapping_mul(2654435761) % ENTRIES_PER_RUN;
    
    // Crash roughly 40% of the time, at varying points
    if hash < ENTRIES_PER_RUN * 2 / 5 {
        // Crash at different points: early, middle, or late
        (hash.wrapping_mul(7)) % ENTRIES_PER_RUN
    } else {
        ENTRIES_PER_RUN + 1 // No crash
    }
}

/// Run the recovery phase: scan log and report state.
fn run_recovery_phase() {
    let path = Path::new(LOG_PATH);

    match LogRecovery::open(path) {
        Ok(Some(recovery)) => {
            println!("Running recovery scan...");
            match recovery.scan() {
                Ok(outcome) => {
                    match outcome {
                        RecoveryOutcome::CleanEmpty { .. } => {
                            println!("Recovery: CLEAN (empty log)");
                            println!("Last valid index: none");
                        }
                        RecoveryOutcome::Clean {
                            last_index,
                            next_offset,
                            highest_view,
                            ..
                        } => {
                            println!("Recovery: CLEAN");
                            println!("Last valid index: {}", last_index);
                            println!("Next offset: {}", next_offset);
                            println!("Highest view: {}", highest_view);
                        }
                        RecoveryOutcome::Truncated {
                            last_valid_index,
                            truncated_at,
                            new_offset,
                            highest_view,
                            ..
                        } => {
                            println!("Recovery: TRUNCATED (torn write repaired)");
                            println!("Last valid index: {}", last_valid_index);
                            println!("Truncated at offset: {}", truncated_at);
                            println!("New write offset: {}", new_offset);
                            println!("Highest view: {}", highest_view);
                        }
                    }
                    process::exit(0);
                }
                Err(e) => {
                    eprintln!("FATAL CORRUPTION: {}", e);
                    process::exit(1);
                }
            }
        }
        Ok(None) => {
            println!("No log file found.");
            process::exit(0);
        }
        Err(e) => {
            eprintln!("FATAL: Failed to open log: {}", e);
            process::exit(1);
        }
    }
}

/// Run a full crash test cycle.
fn run_crash_test() {
    println!("=== chr Crash Test Harness ===\n");

    let path = Path::new(LOG_PATH);

    // Phase 1: Clean start
    println!("Phase 1: Cleaning previous state...");
    let _ = fs::remove_file(path);

    // Phase 2: Write entries (multiple rounds with potential crashes)
    println!("\nPhase 2: Writing entries...");
    for round in 0..5 {
        println!("\n--- Round {} ---", round + 1);

        // Fork a child process to do the writing
        // The child may "crash" (exit early)
        let status = run_write_subprocess();

        if !status.success() {
            eprintln!("Write subprocess failed with non-zero exit");
            process::exit(1);
        }

        // Run recovery after each round
        println!("\nRunning recovery...");
        match LogRecovery::open(path) {
            Ok(Some(recovery)) => match recovery.scan() {
                Ok(outcome) => {
                    print_outcome(&outcome);
                }
                Err(e) => {
                    eprintln!("FATAL CORRUPTION: {}", e);
                    process::exit(1);
                }
            },
            Ok(None) => {
                println!("No log file (expected after clean)");
            }
            Err(e) => {
                eprintln!("FATAL: Failed to open log: {}", e);
                process::exit(1);
            }
        }
    }

    println!("\n=== Crash Test Complete ===");
    println!("All rounds passed. Log integrity verified.");
}

fn run_write_subprocess() -> process::ExitStatus {
    let exe = env::current_exe().expect("Failed to get current executable");
    process::Command::new(exe)
        .arg("write")
        .status()
        .expect("Failed to run write subprocess")
}

fn print_outcome(outcome: &RecoveryOutcome) {
    match outcome {
        RecoveryOutcome::CleanEmpty { .. } => {
            println!("  Status: CLEAN (empty)");
        }
        RecoveryOutcome::Clean { last_index, .. } => {
            println!("  Status: CLEAN");
            println!("  Last valid index: {}", last_index);
        }
        RecoveryOutcome::Truncated {
            last_valid_index,
            truncated_at,
            ..
        } => {
            println!("  Status: TRUNCATED (torn write repaired)");
            println!("  Last valid index: {}", last_valid_index);
            println!("  Truncated at: {}", truncated_at);
        }
    }
}