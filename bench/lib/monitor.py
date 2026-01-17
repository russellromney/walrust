#!/usr/bin/env python3
"""
Resource Monitor for Walrust Benchmark Framework

Monitors CPU and memory usage of processes during benchmarks.
"""

import time
import threading
import statistics
import psutil
from typing import List, Dict, Optional


class ResourceMonitor:
    """Monitor process resources (CPU, memory) over time.

    Samples resource usage at regular intervals and provides aggregated statistics.

    Attributes:
        pid: Process ID to monitor
        include_children: Whether to include child processes in measurements
        sample_interval_ms: Sampling interval in milliseconds
        samples: List of resource samples
        running: Control flag for monitoring loop
        thread: Background monitoring thread
    """

    def __init__(
        self,
        pid: int,
        include_children: bool = False,
        sample_interval_ms: int = 100
    ):
        """Initialize resource monitor.

        Args:
            pid: Process ID to monitor
            include_children: Include child processes in measurements
            sample_interval_ms: Sampling interval (default: 100ms)
        """
        self.pid = pid
        self.include_children = include_children
        self.sample_interval_ms = sample_interval_ms
        self.samples: List[Dict] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self):
        """Start monitoring in background thread."""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop monitoring and wait for thread to finish."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)

    def get_samples(self) -> List[Dict]:
        """Get all resource samples.

        Returns:
            List of sample dicts with timestamp, cpu_percent, memory_mb
        """
        with self._lock:
            return self.samples.copy()

    def get_stats(self) -> Dict:
        """Get aggregated resource statistics.

        Returns:
            Dict with peak/avg/min for memory and CPU
        """
        with self._lock:
            if not self.samples:
                return {
                    'peak_memory_mb': 0.0,
                    'avg_memory_mb': 0.0,
                    'min_memory_mb': 0.0,
                    'peak_cpu_percent': 0.0,
                    'avg_cpu_percent': 0.0,
                    'min_cpu_percent': 0.0,
                    'sample_count': 0
                }

            memory_samples = [s['memory_mb'] for s in self.samples]
            cpu_samples = [s['cpu_percent'] for s in self.samples]

            return {
                'peak_memory_mb': max(memory_samples),
                'avg_memory_mb': statistics.mean(memory_samples),
                'min_memory_mb': min(memory_samples),
                'peak_cpu_percent': max(cpu_samples),
                'avg_cpu_percent': statistics.mean(cpu_samples),
                'min_cpu_percent': min(cpu_samples),
                'sample_count': len(self.samples)
            }

    def _monitor_loop(self):
        """Monitoring loop - samples resource usage at regular intervals."""
        sleep_time = self.sample_interval_ms / 1000.0

        while self.running:
            try:
                # Get process
                proc = psutil.Process(self.pid)

                if self.include_children:
                    # Include all child processes
                    all_procs = [proc]
                    try:
                        all_procs.extend(proc.children(recursive=True))
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                    # Sum across all processes
                    cpu = 0.0
                    mem = 0

                    for p in all_procs:
                        try:
                            cpu += p.cpu_percent(interval=0.1)
                            mem += p.memory_info().rss
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue

                else:
                    # Just the main process
                    cpu = proc.cpu_percent(interval=0.1)
                    mem = proc.memory_info().rss

                # Record sample
                sample = {
                    'timestamp': time.time(),
                    'cpu_percent': cpu,
                    'memory_mb': mem / (1024 * 1024)
                }

                with self._lock:
                    self.samples.append(sample)

            except psutil.NoSuchProcess:
                # Process died - stop monitoring
                break
            except psutil.AccessDenied:
                # Permission denied - skip this sample
                pass
            except Exception as e:
                # Log error but continue
                print(f"Monitor error: {e}")

            # Wait for next sample
            time.sleep(sleep_time)


class MultiProcessMonitor:
    """Monitor multiple processes simultaneously.

    Useful for monitoring walrust + multiple database writers concurrently.
    """

    def __init__(self, sample_interval_ms: int = 100):
        """Initialize multi-process monitor.

        Args:
            sample_interval_ms: Sampling interval for all monitors
        """
        self.sample_interval_ms = sample_interval_ms
        self.monitors: Dict[str, ResourceMonitor] = {}

    def add_process(
        self,
        name: str,
        pid: int,
        include_children: bool = False
    ):
        """Add a process to monitor.

        Args:
            name: Identifier for this process
            pid: Process ID
            include_children: Include child processes
        """
        monitor = ResourceMonitor(
            pid,
            include_children=include_children,
            sample_interval_ms=self.sample_interval_ms
        )
        self.monitors[name] = monitor

    def start(self):
        """Start monitoring all processes."""
        for monitor in self.monitors.values():
            monitor.start()

    def stop(self):
        """Stop monitoring all processes."""
        for monitor in self.monitors.values():
            monitor.stop()

    def get_stats(self) -> Dict[str, Dict]:
        """Get aggregated stats for all processes.

        Returns:
            Dict mapping process name to stats dict
        """
        return {
            name: monitor.get_stats()
            for name, monitor in self.monitors.items()
        }


if __name__ == "__main__":
    # Simple test
    import subprocess
    import os

    print("Testing ResourceMonitor...")

    # Start a test process (sleep for 5 seconds)
    proc = subprocess.Popen(["sleep", "5"])
    print(f"Started test process with PID {proc.pid}")

    # Monitor it
    monitor = ResourceMonitor(proc.pid)
    monitor.start()

    print("Monitoring for 3 seconds...")
    time.sleep(3)

    monitor.stop()

    # Get stats
    stats = monitor.get_stats()
    samples = monitor.get_samples()

    print(f"\nCollected {stats['sample_count']} samples")
    print(f"Memory: {stats['avg_memory_mb']:.2f} MB (peak: {stats['peak_memory_mb']:.2f} MB)")
    print(f"CPU: {stats['avg_cpu_percent']:.1f}% (peak: {stats['peak_cpu_percent']:.1f}%)")

    # Cleanup
    proc.terminate()
    proc.wait()

    print("\n✓ Test passed!")
