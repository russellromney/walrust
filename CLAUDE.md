# walrust — agent config

SQLite→S3 replication. Durability tool: correctness rules are absolute here.
General rules: see `~/.claude/CLAUDE.md` (non-negotiables apply, especially
visible-failures and revert-proof tests).

## Build & test

- `make test` = `cargo test --workspace` with Soup-injected creds → **live Tigris S3**.
  CI runs `make test USE_SOUP=0` against MinIO. S3-gated tests skip (early-return)
  when none of `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL` / `AWS_ACCESS_KEY_ID` is set —
  never weaken this gating.
- Iterate with the narrowest target (`cargo test -p walrust-core --lib <filter>`);
  one full workspace run at the end. See global Build discipline.
- Agents in worktrees: export `CARGO_TARGET_DIR=/Users/russellromney/Documents/Github/walrust/target`
  and `--offline` on every cargo invocation; create worktrees as siblings in `Github/`.
- `.cargo/config.toml` here is **gitignored** and carries two local-dev things:
  `rustc-wrapper = sccache` and the `[patch]` of `hadb-*` to `../hadb`. Worktrees don't
  inherit it — give them a wrapper-only copy (never the patch: it changes dependency
  resolution vs the pinned rev in Cargo.toml).
- E2E deadlines are WAN-sensitive: `WALRUST_E2E_DEADLINE_SECS` overrides the poll
  deadline if live-Tigris runs flake under load.

## Correctness ledger

- Fixed-finding history (every finding named its proving test) lives in
  CHANGELOG.md and git history — the old `ADVERSARIAL_REVIEW*.md` ledgers were
  removed 2026-07-11. Known residual risks live in ROADMAP.md's "Residual risk
  register" (R1–R3): update it when touching anything it covers — nothing
  vanishes silently.
- Never weaken: the racing-checkpoint E2Es (`tests/production_e2e.rs`), the two-writer
  split-brain test, the fenced-follower rejection tests, or SIGKILL crash tests. They
  were each proven load-bearing by neutering; keep them that way.
- Any change to durability paths (WAL read, checkpoint gating, upload cursor, restore
  chain, fenced publish/follow) needs a revert-proof test and, before merge, the full
  adversarial gate (see global Ship glue).

## Layout notes

- Engine lives in `crates/walrust-core` (including `legacy_*` modules from the old
  `src/` tree); `src/` is the CLI plus re-export shims. Don't add logic to shims.
- Core `ltx.rs` implements **HADBP**, not Litestream LTX (checksum-incompatible);
  the `ltx` name is a legacy alias. Don't claim litestream compatibility in docs.
- `hadb-*` deps are pinned to a git rev in `Cargo.toml`; bump the rev deliberately,
  never float the branch.
