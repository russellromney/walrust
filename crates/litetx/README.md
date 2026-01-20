# litetx (walrust fork)

Local fork of [superfly/ltx-rs](https://github.com/superfly/ltx-rs) with litestream compatibility fixes.

## Changes from upstream

- Added `NO_CHECKSUM` flag (0x02) support for reading litestream-produced LTX files
- Updated header validation to skip checksum checks when NO_CHECKSUM is set

## Why fork?

Litestream uses `HeaderFlagNoChecksum = 0x00000002` for files that don't track checksums.
The upstream litetx crate only recognizes `COMPRESS_LZ4 = 0x01`, causing decode failures.

This fork enables walrust to read litestream backups for migration purposes.
