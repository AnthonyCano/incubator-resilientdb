#!/usr/bin/env python3
"""
Parse per-shard primary logs and compute server-side throughput / latency
in the same style as Owen's deliverable (Avg TPS, Max TPS, Avg latency).

Usage:
  python3 parse_server_stats.py <logs_dir> [shard_ids...]

Defaults to shard_ids = 1 5 9 13 (the four primaries) and
logs_dir = /app/scripts/deploy/config_out_sharded/logs inside the container.

Reads:
  - Periodic stats data lines of the form
        "... total request:N txn:N ... time:T ..."
    emitted every ~5 s by stats.cpp:424.
  - "req client latency:X" lines emitted by stats.cpp:474.

Outputs a Markdown table to stdout.
"""

import os
import re
import statistics
import sys
from typing import Dict, List, Tuple

STATS_RE = re.compile(
    r"total request:(\d+)\s+txn:(\d+).*?\btime:(\d+)"
)
LAT_RE = re.compile(r"req client latency:([0-9.eE+-]+)")
# Each monitor window is 5 s (see stats.cpp); confirmed by `time:N`
# increments of 5 between consecutive lines.
WINDOW_SEC = 5.0


def parse_log(path: str) -> Tuple[List[int], List[int], List[float]]:
    """Return per-window (requests, txns, latencies_s) lists from a log file."""
    requests, txns, latencies = [], [], []
    with open(path, "r", errors="ignore") as f:
        for line in f:
            m = STATS_RE.search(line)
            if m:
                requests.append(int(m.group(1)))
                txns.append(int(m.group(2)))
                continue
            m = LAT_RE.search(line)
            if m:
                try:
                    latencies.append(float(m.group(1)))
                except ValueError:
                    pass
    return requests, txns, latencies


def summarise(requests: List[int]) -> Tuple[float, float]:
    """Average TPS over active windows + max TPS over any window."""
    tps_per_window = [n / WINDOW_SEC for n in requests if n > 0]
    if not tps_per_window:
        return 0.0, 0.0
    return statistics.mean(tps_per_window), max(tps_per_window)


def fmt_lat(latencies: List[float]) -> str:
    if not latencies:
        return "n/a"
    avg = statistics.mean(latencies)
    return f"{avg:.2e} s"


def main():
    logs_dir = sys.argv[1] if len(sys.argv) > 1 else (
        "/app/scripts/deploy/config_out_sharded/logs")
    shard_ids = (
        [int(x) for x in sys.argv[2:]]
        if len(sys.argv) > 2 else [1, 5, 9, 13])

    rows = []
    avgs, maxs = [], []
    for sid in shard_ids:
        path = os.path.join(logs_dir, f"kv_{sid}.log")
        if not os.path.exists(path):
            rows.append((sid, "-", "-", "missing"))
            continue
        reqs, _, lats = parse_log(path)
        avg, mx = summarise(reqs)
        rows.append((sid, f"{avg:.1f}", f"{mx:.0f}", fmt_lat(lats)))
        if avg > 0:
            avgs.append(avg)
            maxs.append(mx)

    print("| Shard | Primary ID | Avg TPS | Max TPS | Avg latency |")
    print("|------:|-----------:|--------:|--------:|------------:|")
    for i, (sid, avg, mx, lat) in enumerate(rows, start=1):
        print(f"| {i}     | {sid:>10} | {avg:>7} | {mx:>7} | {lat:>11} |")
    if avgs:
        mean_avg = statistics.mean(avgs)
        mean_max = statistics.mean(maxs)
        print(f"| Mean  |            | {mean_avg:7.1f} | {mean_max:7.1f} | "
              f"            |")
    sum_avg = sum(float(r[1]) for r in rows if r[1] not in ("-", "missing"))
    print(f"\nAggregate cluster Avg TPS: {sum_avg:.1f}")


if __name__ == "__main__":
    main()
