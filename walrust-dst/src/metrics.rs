//! Chaos testing metrics and MTBF tracking
//!
//! Provides metrics collection and reporting for continuous chaos testing:
//! - MTBF (Mean Time Between Failures) calculation
//! - Test duration tracking
//! - Failure seed collection for regression testing
//! - Resource usage monitoring (memory, file descriptors)

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Result of a single chaos test iteration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterationResult {
    /// Iteration number
    pub iteration: u64,
    /// Seed used for this iteration
    pub seed: u64,
    /// Whether the test passed
    pub passed: bool,
    /// Duration of this iteration
    pub duration_ms: u64,
    /// Error message if failed
    pub error: Option<String>,
    /// Memory usage at end (bytes)
    pub memory_bytes: Option<u64>,
    /// Open file descriptors
    pub open_fds: Option<u32>,
}

/// Aggregated metrics from chaos testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosMetrics {
    /// Test name
    pub test_name: String,
    /// Total iterations run
    pub total_iterations: u64,
    /// Number of passed iterations
    pub passed: u64,
    /// Number of failed iterations
    pub failed: u64,
    /// Total test duration
    pub total_duration_ms: u64,
    /// Mean time between failures (ms)
    pub mtbf_ms: Option<u64>,
    /// Seeds that caused failures (for regression)
    pub failure_seeds: Vec<u64>,
    /// Average iteration duration
    pub avg_iteration_ms: u64,
    /// Min iteration duration
    pub min_iteration_ms: u64,
    /// Max iteration duration
    pub max_iteration_ms: u64,
    /// Peak memory usage observed
    pub peak_memory_bytes: Option<u64>,
    /// Max file descriptors observed
    pub max_fds: Option<u32>,
    /// Recovery time stats (if applicable)
    pub recovery_stats: Option<RecoveryStats>,
}

/// Recovery time statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStats {
    /// Average recovery time (ms)
    pub avg_recovery_ms: u64,
    /// Max recovery time (ms)
    pub max_recovery_ms: u64,
    /// Min recovery time (ms)
    pub min_recovery_ms: u64,
    /// Number of recovery operations measured
    pub count: u64,
}

/// Tracks metrics during continuous chaos testing
pub struct MetricsCollector {
    test_name: String,
    start_time: Instant,
    iterations: Vec<IterationResult>,
    failure_times: VecDeque<Instant>,
    recovery_times: Vec<u64>,
    last_failure_time: Option<Instant>,
    peak_memory: Option<u64>,
    max_fds: Option<u32>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(test_name: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            start_time: Instant::now(),
            iterations: Vec::new(),
            failure_times: VecDeque::new(),
            recovery_times: Vec::new(),
            last_failure_time: None,
            peak_memory: None,
            max_fds: None,
        }
    }

    /// Record a test iteration result
    pub fn record_iteration(&mut self, result: IterationResult) {
        // Track failures for MTBF
        if !result.passed {
            self.failure_times.push_back(Instant::now());
            self.last_failure_time = Some(Instant::now());
        }

        // Track peak memory
        if let Some(mem) = result.memory_bytes {
            self.peak_memory = Some(self.peak_memory.unwrap_or(0).max(mem));
        }

        // Track max FDs
        if let Some(fds) = result.open_fds {
            self.max_fds = Some(self.max_fds.unwrap_or(0).max(fds));
        }

        self.iterations.push(result);
    }

    /// Record a recovery time measurement
    pub fn record_recovery(&mut self, duration_ms: u64) {
        self.recovery_times.push(duration_ms);
    }

    /// Get current iteration count
    pub fn iteration_count(&self) -> u64 {
        self.iterations.len() as u64
    }

    /// Get current pass rate
    pub fn pass_rate(&self) -> f64 {
        if self.iterations.is_empty() {
            return 0.0;
        }
        let passed = self.iterations.iter().filter(|r| r.passed).count();
        passed as f64 / self.iterations.len() as f64 * 100.0
    }

    /// Calculate MTBF (Mean Time Between Failures)
    fn calculate_mtbf(&self) -> Option<u64> {
        let failures: Vec<_> = self
            .iterations
            .iter()
            .enumerate()
            .filter(|(_, r)| !r.passed)
            .collect();

        if failures.len() < 2 {
            return None;
        }

        // Calculate time between consecutive failures
        let mut intervals: Vec<u64> = Vec::new();
        for i in 1..failures.len() {
            let prev_idx = failures[i - 1].0;
            let curr_idx = failures[i].0;

            // Sum durations between failures
            let interval: u64 = self.iterations[prev_idx..curr_idx]
                .iter()
                .map(|r| r.duration_ms)
                .sum();
            intervals.push(interval);
        }

        if intervals.is_empty() {
            return None;
        }

        Some(intervals.iter().sum::<u64>() / intervals.len() as u64)
    }

    /// Build recovery stats
    fn build_recovery_stats(&self) -> Option<RecoveryStats> {
        if self.recovery_times.is_empty() {
            return None;
        }

        let sum: u64 = self.recovery_times.iter().sum();
        let count = self.recovery_times.len() as u64;
        let max = *self.recovery_times.iter().max().unwrap();
        let min = *self.recovery_times.iter().min().unwrap();

        Some(RecoveryStats {
            avg_recovery_ms: sum / count,
            max_recovery_ms: max,
            min_recovery_ms: min,
            count,
        })
    }

    /// Generate final metrics report
    pub fn finalize(&self) -> ChaosMetrics {
        let total_duration = self.start_time.elapsed().as_millis() as u64;
        let passed = self.iterations.iter().filter(|r| r.passed).count() as u64;
        let failed = self.iterations.len() as u64 - passed;

        let durations: Vec<u64> = self.iterations.iter().map(|r| r.duration_ms).collect();
        let (avg, min, max) = if durations.is_empty() {
            (0, 0, 0)
        } else {
            let sum: u64 = durations.iter().sum();
            (
                sum / durations.len() as u64,
                *durations.iter().min().unwrap(),
                *durations.iter().max().unwrap(),
            )
        };

        let failure_seeds: Vec<u64> = self
            .iterations
            .iter()
            .filter(|r| !r.passed)
            .map(|r| r.seed)
            .collect();

        ChaosMetrics {
            test_name: self.test_name.clone(),
            total_iterations: self.iterations.len() as u64,
            passed,
            failed,
            total_duration_ms: total_duration,
            mtbf_ms: self.calculate_mtbf(),
            failure_seeds,
            avg_iteration_ms: avg,
            min_iteration_ms: min,
            max_iteration_ms: max,
            peak_memory_bytes: self.peak_memory,
            max_fds: self.max_fds,
            recovery_stats: self.build_recovery_stats(),
        }
    }
}

/// Get current process memory usage (RSS) on supported platforms
#[cfg(target_os = "macos")]
pub fn get_memory_usage() -> Option<u64> {
    use std::process::Command;

    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;

    let rss_kb: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()?;

    Some(rss_kb * 1024) // Convert KB to bytes
}

#[cfg(target_os = "linux")]
pub fn get_memory_usage() -> Option<u64> {
    use std::fs;

    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: u64 = parts[1].parse().ok()?;
                return Some(kb * 1024);
            }
        }
    }
    None
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn get_memory_usage() -> Option<u64> {
    None
}

/// Get current open file descriptor count
#[cfg(unix)]
pub fn get_open_fds() -> Option<u32> {
    use std::fs;

    let fd_dir = format!("/proc/{}/fd", std::process::id());
    if let Ok(entries) = fs::read_dir(&fd_dir) {
        Some(entries.count() as u32)
    } else {
        // macOS fallback
        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            let output = Command::new("lsof")
                .args(["-p", &std::process::id().to_string()])
                .output()
                .ok()?;
            let count = String::from_utf8_lossy(&output.stdout)
                .lines()
                .count()
                .saturating_sub(1) as u32;
            Some(count)
        }
        #[cfg(not(target_os = "macos"))]
        None
    }
}

#[cfg(not(unix))]
pub fn get_open_fds() -> Option<u32> {
    None
}

/// Format metrics as a human-readable report
pub fn format_report(metrics: &ChaosMetrics) -> String {
    let mut report = String::new();

    report.push_str(&format!(
        "\n{}\n{}\n\n",
        "=".repeat(60),
        format!("  Chaos Test Report: {}", metrics.test_name)
    ));

    report.push_str(&format!(
        "  Iterations: {} total ({} passed, {} failed)\n",
        metrics.total_iterations, metrics.passed, metrics.failed
    ));

    let pass_rate = if metrics.total_iterations > 0 {
        metrics.passed as f64 / metrics.total_iterations as f64 * 100.0
    } else {
        0.0
    };
    report.push_str(&format!("  Pass Rate:  {:.2}%\n", pass_rate));

    report.push_str(&format!(
        "  Duration:   {}ms (avg: {}ms, min: {}ms, max: {}ms)\n",
        metrics.total_duration_ms,
        metrics.avg_iteration_ms,
        metrics.min_iteration_ms,
        metrics.max_iteration_ms
    ));

    if let Some(mtbf) = metrics.mtbf_ms {
        report.push_str(&format!(
            "  MTBF:       {}ms ({:.2} iterations avg)\n",
            mtbf,
            mtbf as f64 / metrics.avg_iteration_ms.max(1) as f64
        ));
    } else {
        report.push_str("  MTBF:       N/A (fewer than 2 failures)\n");
    }

    if let Some(peak_mem) = metrics.peak_memory_bytes {
        report.push_str(&format!(
            "  Peak Memory: {:.2} MB\n",
            peak_mem as f64 / 1024.0 / 1024.0
        ));
    }

    if let Some(max_fds) = metrics.max_fds {
        report.push_str(&format!("  Max FDs:    {}\n", max_fds));
    }

    if let Some(ref recovery) = metrics.recovery_stats {
        report.push_str(&format!(
            "  Recovery:   avg {}ms, max {}ms (n={})\n",
            recovery.avg_recovery_ms, recovery.max_recovery_ms, recovery.count
        ));
    }

    if !metrics.failure_seeds.is_empty() {
        report.push_str(&format!("\n  Failure Seeds (for replay):\n"));
        for (i, seed) in metrics.failure_seeds.iter().take(10).enumerate() {
            report.push_str(&format!("    {}. {:#x}\n", i + 1, seed));
        }
        if metrics.failure_seeds.len() > 10 {
            report.push_str(&format!(
                "    ... and {} more\n",
                metrics.failure_seeds.len() - 10
            ));
        }
    }

    report.push_str(&format!("{}\n", "=".repeat(60)));

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_basic() {
        let mut collector = MetricsCollector::new("test");

        collector.record_iteration(IterationResult {
            iteration: 0,
            seed: 42,
            passed: true,
            duration_ms: 100,
            error: None,
            memory_bytes: Some(1000),
            open_fds: Some(10),
        });

        collector.record_iteration(IterationResult {
            iteration: 1,
            seed: 43,
            passed: false,
            duration_ms: 150,
            error: Some("test error".to_string()),
            memory_bytes: Some(2000),
            open_fds: Some(15),
        });

        let metrics = collector.finalize();
        assert_eq!(metrics.total_iterations, 2);
        assert_eq!(metrics.passed, 1);
        assert_eq!(metrics.failed, 1);
        assert_eq!(metrics.peak_memory_bytes, Some(2000));
        assert_eq!(metrics.max_fds, Some(15));
    }

    #[test]
    fn test_mtbf_calculation() {
        let mut collector = MetricsCollector::new("test");

        // Pass, Fail, Pass, Pass, Fail
        let results = vec![
            (true, 100),
            (false, 100),
            (true, 100),
            (true, 100),
            (false, 100),
        ];

        for (i, (passed, duration)) in results.iter().enumerate() {
            collector.record_iteration(IterationResult {
                iteration: i as u64,
                seed: i as u64,
                passed: *passed,
                duration_ms: *duration,
                error: if !passed {
                    Some("error".to_string())
                } else {
                    None
                },
                memory_bytes: None,
                open_fds: None,
            });
        }

        let metrics = collector.finalize();
        // Between failure at idx 1 and failure at idx 4: 100+100+100 = 300ms
        assert_eq!(metrics.mtbf_ms, Some(300));
    }

    #[test]
    fn test_recovery_stats() {
        let mut collector = MetricsCollector::new("test");

        collector.record_recovery(100);
        collector.record_recovery(200);
        collector.record_recovery(150);

        let metrics = collector.finalize();
        let stats = metrics.recovery_stats.unwrap();
        assert_eq!(stats.avg_recovery_ms, 150);
        assert_eq!(stats.max_recovery_ms, 200);
        assert_eq!(stats.min_recovery_ms, 100);
        assert_eq!(stats.count, 3);
    }

    #[test]
    fn test_format_report() {
        let metrics = ChaosMetrics {
            test_name: "test_chaos".to_string(),
            total_iterations: 100,
            passed: 95,
            failed: 5,
            total_duration_ms: 10000,
            mtbf_ms: Some(2000),
            failure_seeds: vec![0x1234, 0x5678],
            avg_iteration_ms: 100,
            min_iteration_ms: 50,
            max_iteration_ms: 200,
            peak_memory_bytes: Some(10 * 1024 * 1024),
            max_fds: Some(50),
            recovery_stats: Some(RecoveryStats {
                avg_recovery_ms: 500,
                max_recovery_ms: 1000,
                min_recovery_ms: 200,
                count: 5,
            }),
        };

        let report = format_report(&metrics);
        assert!(report.contains("test_chaos"));
        assert!(report.contains("95.00%"));
        assert!(report.contains("MTBF"));
        assert!(report.contains("0x1234"));
    }
}
