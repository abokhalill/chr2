use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub const fn new() -> Self {
        Counter {
            value: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn reset(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicU64,
}

impl Gauge {
    pub const fn new() -> Self {
        Gauge {
            value: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn set(&self, n: u64) {
        self.value.store(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct Histogram {
    buckets: [AtomicU64; 11],
    sum: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    const BUCKET_BOUNDS: [u64; 10] = [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, u64::MAX];

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

    #[inline]
    pub fn observe(&self, value_us: u64) {
        let bucket_idx = Self::BUCKET_BOUNDS
            .iter()
            .position(|&bound| value_us <= bound)
            .unwrap_or(10);

        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn observe_duration(&self, start: Instant) {
        let elapsed = start.elapsed();
        let us = elapsed.as_micros() as u64;
        self.observe(us);
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum() as f64 / count as f64
        }
    }

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

pub struct EngineMetrics {
    pub entries_written: Counter,
    pub bytes_written: Counter,
    pub write_latency_us: Histogram,
    pub fdatasync_count: Counter,
    pub entries_read: Counter,
    pub read_latency_us: Histogram,
    pub recoveries: Counter,
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

pub struct VsrMetrics {
    pub requests_received: Counter,
    pub requests_rejected: Counter,
    pub entries_committed: Counter,
    pub view_changes: Counter,
    pub fenced_messages: Counter,
    pub current_view: Gauge,
    pub current_role: Gauge,
    pub inflight_requests: Gauge,
    pub replication_lag: Gauge,
    pub prepare_latency_us: Histogram,
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

pub struct ExecutorMetrics {
    pub entries_applied: Counter,
    pub apply_latency_us: Histogram,
    pub snapshots_taken: Counter,
    pub snapshot_latency_us: Histogram,
    pub side_effects_emitted: Counter,
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
        hist.observe(5); // bucket 0 (<=10)
        hist.observe(25); // bucket 1 (<=50)
        hist.observe(75); // bucket 2 (<=100)
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
            hist.observe(5); // bucket 0
            hist.observe(25); // bucket 1
            hist.observe(75); // bucket 2
            hist.observe(250); // bucket 3
            hist.observe(750); // bucket 4
            hist.observe(2500); // bucket 5
            hist.observe(7500); // bucket 6
            hist.observe(25000); // bucket 7
            hist.observe(75000); // bucket 8
            hist.observe(150000); // bucket 9
        }

        assert_eq!(hist.count(), 100);

        // p50 should be in bucket 4 (<=1000)
        assert_eq!(hist.percentile(50.0), 1000);

        // p90 should be in bucket 8 (<=100000)
        assert_eq!(hist.percentile(90.0), 100000);
    }
}
