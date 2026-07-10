#!/usr/bin/env python3
"""Summarize multidb-rss RSS curves into a table + results.json.

Reads run-meta.json and rss-<tool>-n<N>-<shape>.tsv (epoch<TAB>rss_kb) from
the run directory. "RSS at end" is the median of the last 3 samples of each
cell, which smooths a single unlucky sample without hiding growth. The claim
under test is constant-vs-linear scaling in database count, so the table is
organized as N rows x tool columns per shape.
"""

import json
import statistics
import sys
from pathlib import Path


def parse_rss(path):
    vals = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.split()
            if len(parts) >= 2:
                vals.append(int(parts[1]) / 1024.0)  # KB -> MB
    if not vals:
        return None
    return {
        "samples": len(vals),
        "min_mb": round(min(vals), 1),
        "median_mb": round(statistics.median(vals), 1),
        "max_mb": round(max(vals), 1),
        "end_mb": round(statistics.median(vals[-3:]), 1),
    }


def main():
    run_dir = Path(sys.argv[1])
    meta = json.loads((run_dir / "run-meta.json").read_text())
    tools = ["walrust", "litestream"]

    cells = {}
    for shape in meta["shapes"]:
        for n in meta["db_counts"]:
            for tool in tools:
                path = run_dir / f"rss-{tool}-n{n}-{shape}.tsv"
                if path.exists():
                    cells[f"{tool}-n{n}-{shape}"] = parse_rss(path)

    results = {
        "benchmark": "multidb-rss",
        "schema_version": 1,
        "meta": meta,
        "rss": cells,
    }
    (run_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")

    lines = ["", "== multi-database RSS scaling (MB) =="]
    widths = [8, 6, 22, 22]
    for shape in meta["shapes"]:
        lines.append("")
        lines.append(f"shape: {shape}")
        lines.append(
            fmt_row(["", "N", "walrust end (min/max)", "litestream end (min/max)"], widths)
        )
        lines.append(fmt_row(["", *["-" * w for w in widths[1:]]], widths))
        for n in meta["db_counts"]:
            row = ["", n]
            for tool in tools:
                cell = cells.get(f"{tool}-n{n}-{shape}")
                if cell:
                    row.append(f"{cell['end_mb']} ({cell['min_mb']}/{cell['max_mb']})")
                else:
                    row.append("-")
            lines.append(fmt_row(row, widths))
    lines.append("")
    lines.append(f"results json: {run_dir / 'results.json'}")
    print("\n".join(lines))


def fmt_row(cols, widths):
    return "  ".join(str(c).ljust(w) for c, w in zip(cols, widths)).rstrip()


if __name__ == "__main__":
    main()
