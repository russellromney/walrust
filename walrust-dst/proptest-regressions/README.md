# proptest regression corpus

Failures found by the walrust-dst property suites persist here as a permanent
corpus: proptest re-runs every seed in these files before generating novel
cases, so every past failure is retried on every run. Do not delete entries —
commit new ones as they are found.

`state_machine.txt` belongs to `walrust-dst/src/state_machine.rs` (the
model-based state-machine harness; run with
`cargo test -p walrust-dst state_machine`). Its first pinned seed is the
shrunk E2 catch-proof sequence: with the bridge-snapshot rescue disabled in
`walrust-core::legacy_manifest::plan_legacy_compaction`, compaction deletes a
load-bearing middle snapshot and point-in-time restore fails on
"restore incremental gap". With the rescue intact the seed passes.
