//! Lightweight metrics for observability.
//!
//! Provides atomic counters and histograms for tracking system behavior.
//! Designed to be lock-free and suitable for high-throughput paths.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Atomic counter for tracking events.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    /// Create a new counter initialized to zero.
    pub const fn new() -> Self {
        Counter {
            value: AtomicU64::new(0),
        }
    }

    /// Increment the counter by 1.
    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the counter by a specific amount.
    #[inline]
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the counter to zero and return the previous value.
    #[inline]
    pub fn reset(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }
}

/// Atomic gauge for tracking current values (can go up or down).
#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicU64,
}

impl Gauge {
    /// Create a new gauge initialized to zero.
    pub const fn new() -> Self {
        Gauge {
            value: AtomicU64::new(0),
        }
    }

    /// Set the gauge to a specific value.
    #[inline]
    pub fn set(&self, n: u64) {
        self.value.store(n, Ordering::Relaxed);
    }

    /// Increment the gauge by 1.
    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the gauge by 1.
    #[inline]
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Simple histogram using fixed buckets for latency tracking.
/// Buckets are in microseconds: [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, +Inf]
#[derive(Debug)]
pub struct Histogram {
    /// Bucket counts (10 buckets + overflow)
    buckets: [AtomicU64; 11],
    /// Sum of all observed values (for computing mean)
    sum: AtomicU64,
    /// Count of all observations
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    /// Bucket boundaries in microseconds.
    const BUCKET_BOUNDS: [u64; 10] = [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, u64::MAX];

    /// Create a new histogram.
    pub const fn new() -> Self {
        Histogram {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record a value in microseconds.
    #[inline]
    pub fn observe(&self, value_us: u64) {
        // Find the appropriate bucket
        let bucket_idx = Self::BUCKET_BOUNDS
            .iter()
            .position(|&bound| value_us <= bound)
            .unwrap_or(10);

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a duration (converts to microseconds).
    #[inline]
    pub fn observe_duration(&self, start: Instant) {
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as u64;
        self.observe(us);
    }

    /// Get the total count of observations.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of all observations.
    #[inline]
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Get the mean value (returns 0 if no observations).
    #[inline]
    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum() as f64 / count as f64
        }
    }

    /// Get bucket counts as an array.
    pub fn bucket_counts(&self) -> [u64; 11] {
        [
            self.buckets[0].load(Ordering::Relaxed),
            self.buckets[1].load(Ordering::Relaxed),
            self.buckets[2].load(Ordering::Relaxed),
            self.buckets[3].load(Ordering::Relaxed),
            self.buckets[4].load(Ordering::Relaxed),
            self.buckets[5].load(Ordering::Relaxed),
            self.buckets[6].load(Ordering::Relaxed),
            self.buckets[7].load(Ordering::Relaxed),
            self.buckets[8].load(Ordering::Relaxed),
            self.buckets[9].load(Ordering::Relaxed),
            self.buckets[10].load(Ordering::Relaxed),
        ]
    }

    /// Get an approximate percentile (p50, p90, p99, etc.).
    /// Returns the upper bound of the bucket containing that percentile.
    pub fn percentile(&self, p: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }

        let target = (total as f64 * p / 100.0).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return if i < 10 {
                    Self::BUCKET_BOUNDS[i]
                } else {
                    u64::MAX
                };
            }
        }

        u64::MAX
    }
}

/// Engine-level metrics.
pub struct EngineMetrics {
    /// Total entries written
    pub entries_written: Counter,
    /// Total bytes written
    pub bytes_written: Counter,
    /// Write latency histogram (microseconds)
    pub write_latency_us: Histogram,
    /// Total fdatasync calls
    pub fdatasync_count: Counter,
    /// Total entries read
    pub entries_read: Counter,
    /// Read latency histogram (microseconds)
    pub read_latency_us: Histogram,
    /// Recovery operations
    pub recoveries: Counter,
    /// Truncated entries during recovery
    pub truncated_entries: Counter,
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineMetrics {
    pub const fn new() -> Self {
        EngineMetrics {
            entries_written: Counter::new(),
            bytes_written: Counter::new(),
            write_latency_us: Histogram::new(),
            fdatasync_count: Counter::new(),
            entries_read: Counter::new(),
            read_latency_us: Histogram::new(),
            recoveries: Counter::new(),
            truncated_entries: Counter::new(),
        }
    }
}

/// VSR-level metrics.
pub struct VsrMetrics {
    /// Client requests received
    pub requests_received: Counter,
    /// Client requests rejected (overload)
    pub requests_rejected: Counter,
    /// Entries committed
    pub entries_committed: Counter,
    /// View changes initiated
    pub view_changes: Counter,
    /// Fenced messages rejected
    pub fenced_messages: Counter,
    /// Current view (gauge)
    pub current_view: Gauge,
    /// Current role (0=Backup, 1=Primary, 2=ViewChange)
    pub current_role: Gauge,
    /// In-flight requests (gauge)
    pub inflight_requests: Gauge,
    /// Replication lag (gauge)
    pub replication_lag: Gauge,
    /// Prepare latency histogram
    pub prepare_latency_us: Histogram,
    /// Commit latency histogram
    pub commit_latency_us: Histogram,
}

impl Default for VsrMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl VsrMetrics {
    pub const fn new() -> Self {
        VsrMetrics {
            requests_received: Counter::new(),
            requests_rejected: Counter::new(),
            entries_committed: Counter::new(),
            view_changes: Counter::new(),
            fenced_messages: Counter::new(),
            current_view: Gauge::new(),
            current_role: Gauge::new(),
            inflight_requests: Gauge::new(),
            replication_lag: Gauge::new(),
            prepare_latency_us: Histogram::new(),
            commit_latency_us: Histogram::new(),
        }
    }
}

/// Executor-level metrics.
pub struct ExecutorMetrics {
    /// Entries applied
    pub entries_applied: Counter,
    /// Apply latency histogram
    pub apply_latency_us: Histogram,
    /// Snapshots taken
    pub snapshots_taken: Counter,
    /// Snapshot latency histogram
    pub snapshot_latency_us: Histogram,
    /// Side effects emitted
    pub side_effects_emitted: Counter,
    /// Side effects executed
    pub side_effects_executed: Counter,
}

impl Default for ExecutorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorMetrics {
    pub const fn new() -> Self {
        ExecutorMetrics {
            entries_applied: Counter::new(),
            apply_latency_us: Histogram::new(),
            snapshots_taken: Counter::new(),
            snapshot_latency_us: Histogram::new(),
            side_effects_emitted: Counter::new(),
            side_effects_executed: Counter::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.add(5);
        assert_eq!(counter.get(), 6);

        let prev = counter.reset();
        assert_eq!(prev, 6);
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);

        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        gauge.inc();
        assert_eq!(gauge.get(), 11);

        gauge.dec();
        assert_eq!(gauge.get(), 10);
    }

    #[test]
    fn test_histogram() {
        let hist = Histogram::new();

        // Record some values
        hist.observe(5);   // bucket 0 (<=10)
        hist.observe(25);  // bucket 1 (<=50)
        hist.observe(75);  // bucket 2 (<=100)
        hist.observe(250); // bucket 3 (<=500)
        hist.observe(750); // bucket 4 (<=1000)

        assert_eq!(hist.count(), 5);
        assert_eq!(hist.sum(), 5 + 25 + 75 + 250 + 750);

        let buckets = hist.bucket_counts();
        assert_eq!(buckets[0], 1); // <=10
        assert_eq!(buckets[1], 1); // <=50
        assert_eq!(buckets[2], 1); // <=100
        assert_eq!(buckets[3], 1); // <=500
        assert_eq!(buckets[4], 1); // <=1000
    }

    #[test]
    fn test_histogram_percentile() {
        let hist = Histogram::new();

        // Add 100 values, 10 in each of the first 10 buckets
        for _ in 0..10 {
            hist.observe(5);     // bucket 0
            hist.observe(25);    // bucket 1
            hist.observe(75);    // bucket 2
            hist.observe(250);   // bucket 3
            hist.observe(750);   // bucket 4
            hist.observe(2500);  // bucket 5
            hist.observe(7500);  // bucket 6
            hist.observe(25000); // bucket 7
            hist.observe(75000); // bucket 8
            hist.observe(150000);// bucket 9
        }

        assert_eq!(hist.count(), 100);

        // p50 should be in bucket 4 (<=1000)
        assert_eq!(hist.percentile(50.0), 1000);

        // p90 should be in bucket 8 (<=100000)
        assert_eq!(hist.percentile(90.0), 100000);
    }
}
