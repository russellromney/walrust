#!/usr/bin/env python3
"""Turn raw compare-litestream artifacts into a table + results.json.

Reads from the run directory (written by bench/compare-litestream.sh):
  run-meta.json   knobs, prefixes, row counts (written by the shell script)
  trace.jsonl     `mc admin trace --json -v` capture from the MinIO sidecar
  rss-<tool>.tsv  epoch<TAB>rss_kb samples
  lag-<tool>.tsv  epoch<TAB>seq<TAB>lag_seconds|timeout<TAB>method samples

Request counting rules (documented in bench/README.md):
  - counted from the server-side trace, NOT from end-state object listings
  - attributed to a tool by object path prefix; requests without a
    distinguishing path (e.g. bulk deletes against the bucket root) fall
    back to User-Agent attribution learned from path-attributed requests
  - probe/harness traffic (minio-py User-Agent) is excluded

Writes results.json into the run directory and prints a plain-text report.
"""

import json
import statistics
import sys
from pathlib import Path

PUT_APIS = {
    "PutObject",
    "PutObjectPart",
    "NewMultipartUpload",
    "CompleteMultipartUpload",
    "CopyObject",
}
GET_APIS = {"GetObject"}
LIST_APIS = {"ListObjectsV1", "ListObjectsV2", "ListObjectVersions"}
DELETE_APIS = {"DeleteObject", "DeleteMultipleObjects"}
HEAD_APIS = {"HeadObject"}

CLASSES = ["PUT", "GET", "LIST", "DELETE", "HEAD", "OTHER"]


def classify(api):
    name = api.split(".", 1)[-1]
    if name in PUT_APIS:
        return "PUT"
    if name in GET_APIS:
        return "GET"
    if name in LIST_APIS:
        return "LIST"
    if name in DELETE_APIS:
        return "DELETE"
    if name in HEAD_APIS:
        return "HEAD"
    return "OTHER"


def parse_trace(trace_path, bucket, tool_prefixes):
    """tool_prefixes: {tool: object_key_prefix}. Returns per-tool counts."""
    entries = []
    with open(trace_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("type") not in (None, "S3"):
                continue
            api = entry.get("api", "")
            if not api.startswith("s3."):
                continue
            path = entry.get("path", "") or ""
            headers = (entry.get("request") or {}).get("headers") or {}
            ua = headers.get("User-Agent", "")
            entries.append((api, path, ua))

    def tool_for_path(path):
        for tool, key_prefix in tool_prefixes.items():
            if path.startswith(f"/{bucket}/{key_prefix}"):
                return tool
        return None

    # Pass 1: path attribution; learn each tool's SDK family. AWS SDK
    # User-Agents carry per-operation feature flags (e.g. "m/G,Z,e"), so
    # normalize to the first token ("aws-sdk-go-v2/1.37.1") before mapping.
    # A family claimed by both tools is ambiguous and stays unattributed.
    def ua_family(ua):
        return ua.split(" ", 1)[0] if ua else ""

    ua_map = {}
    ambiguous = set()
    for api, path, ua in entries:
        tool = tool_for_path(path)
        family = ua_family(ua)
        if tool and family and "minio-py" not in ua:
            if ua_map.setdefault(family, tool) != tool:
                ambiguous.add(family)
    for family in ambiguous:
        ua_map.pop(family, None)

    counts = {
        tool: {"classes": dict.fromkeys(CLASSES, 0), "apis": {}, "total": 0}
        for tool in tool_prefixes
    }
    excluded_probe = 0
    unattributed = 0
    for api, path, ua in entries:
        if "minio-py" in ua:
            excluded_probe += 1
            continue
        tool = tool_for_path(path) or ua_map.get(ua_family(ua))
        if tool is None:
            unattributed += 1
            continue
        cls = classify(api)
        bucket_counts = counts[tool]
        bucket_counts["classes"][cls] += 1
        bucket_counts["apis"][api] = bucket_counts["apis"].get(api, 0) + 1
        bucket_counts["total"] += 1
    return counts, excluded_probe, unattributed


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
    }


def parse_lag(path):
    lags = []
    timeouts = 0
    methods = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue
            if parts[2] == "timeout":
                timeouts += 1
            else:
                lags.append(float(parts[2]))
            methods[parts[3]] = methods.get(parts[3], 0) + 1
    result = {"samples": len(lags), "timeouts": timeouts, "methods": methods}
    if lags:
        lags.sort()
        result["median_s"] = round(statistics.median(lags), 3)
        p95_idx = max(0, int(round(0.95 * (len(lags) + 1))) - 1)
        result["p95_s"] = round(lags[min(p95_idx, len(lags) - 1)], 3)
        result["max_s"] = round(lags[-1], 3)
    return result


def fmt_row(cols, widths):
    return "  ".join(str(c).ljust(w) for c, w in zip(cols, widths))


def main():
    run_dir = Path(sys.argv[1])
    meta = json.loads((run_dir / "run-meta.json").read_text())
    bucket = meta["bucket"]
    tools = ["walrust", "litestream"]
    tool_prefixes = {t: meta["object_prefix"][t] for t in tools}

    requests, excluded_probe, unattributed = parse_trace(
        run_dir / "trace.jsonl", bucket, tool_prefixes
    )
    rss = {t: parse_rss(run_dir / f"rss-{t}.tsv") for t in tools}
    lag = {t: parse_lag(run_dir / f"lag-{t}.tsv") for t in tools}

    results = {
        "benchmark": "compare-litestream",
        "schema_version": 1,
        "meta": meta,
        "requests": {
            **{t: requests[t] for t in tools},
            "excluded_probe_requests": excluded_probe,
            "unattributed_requests": unattributed,
        },
        "rss": rss,
        "replication_lag": lag,
    }
    (run_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")

    dur = meta["duration_seconds"]
    lines = []
    lines.append("")
    lines.append("== walrust vs litestream: results ==")
    lines.append("")
    widths = [26, 14, 14]
    lines.append(fmt_row(["metric", "walrust", "litestream"], widths))
    lines.append(fmt_row(["-" * w for w in widths], widths))
    lines.append(
        fmt_row(["rows written", meta["rows"]["walrust"], meta["rows"]["litestream"]], widths)
    )
    for cls in CLASSES:
        vals = [requests[t]["classes"][cls] for t in tools]
        if any(vals):
            lines.append(fmt_row([f"S3 {cls} requests", *vals], widths))
    lines.append(
        fmt_row(["S3 total requests", *[requests[t]["total"] for t in tools]], widths)
    )
    lines.append(
        fmt_row(
            ["S3 PUTs per minute", *[
                round(requests[t]["classes"]["PUT"] * 60.0 / dur, 1) for t in tools
            ]],
            widths,
        )
    )
    for label, key in [
        ("lag median (s)", "median_s"),
        ("lag p95 (s)", "p95_s"),
        ("lag samples", "samples"),
        ("lag probe timeouts", "timeouts"),
    ]:
        lines.append(fmt_row([label, *[lag[t].get(key, "-") for t in tools]], widths))
    for label, key in [
        ("RSS min (MB)", "min_mb"),
        ("RSS median (MB)", "median_mb"),
        ("RSS max (MB)", "max_mb"),
    ]:
        lines.append(
            fmt_row([label, *[(rss[t] or {}).get(key, "-") for t in tools]], widths)
        )
    lines.append("")
    lines.append(f"probe requests excluded from counts: {excluded_probe}")
    lines.append(f"unattributed requests (neither tool): {unattributed}")
    lines.append(f"results json: {run_dir / 'results.json'}")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
