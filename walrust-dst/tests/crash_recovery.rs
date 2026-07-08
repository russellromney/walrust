//! CI regression for the real child-process SIGKILL crash-recovery chaos test.
//!
//! Drives the production `walrust-dst chaos --faults crashes` path end to end:
//! it spawns the walrust-dst binary, which itself spawns durable-writer children,
//! SIGKILLs each one mid-write, and reopens the on-disk cache to prove the
//! committed prefix survived and the manifest is not torn. A non-zero exit here
//! means a real process kill left the cache unrecoverable (A12 regression).

use std::process::Command;

#[test]
fn chaos_crashes_survives_real_process_kill() {
    let bin = env!("CARGO_BIN_EXE_walrust-dst");
    let output = Command::new(bin)
        .args([
            "chaos",
            "--faults",
            "crashes",
            "--iterations",
            "10",
            "--seed",
            "7",
        ])
        .output()
        .expect("spawn walrust-dst chaos --faults crashes");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "real-kill crash recovery must pass; exit={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert!(
        stdout.contains("chaos_process_crash_recovery"),
        "expected the real crash-recovery test to run; stdout:\n{stdout}"
    );
}
