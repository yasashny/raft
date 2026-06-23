#!/usr/bin/env python3
"""Render the three required charts from benchmarks/results.csv.

Usage:  python3 benchmarks/plot.py
Outputs (next to the CSV):
  partA_throughput_vs_livenodes.png
  partB_failover_vs_timeout.png
  partC_fsync_latency.png
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "results.csv")


def load():
    with open(CSV, newline="") as f:
        return list(csv.DictReader(f))


def part_a(rows):
    a = [r for r in rows if r["part"] == "A"]
    a.sort(key=lambda r: -int(r["liveNodes"]))
    live = [int(r["liveNodes"]) for r in a]
    thr = [float(r["throughputOpsSec"]) for r in a]
    p95 = [float(r["p95Ms"]) for r in a]
    labels = [f'{n} live' for n in live]

    fig, ax1 = plt.subplots(figsize=(7, 4.5))
    x = range(len(a))
    bars = ax1.bar(x, thr, color="#357ABD", width=0.55)
    ax1.set_ylabel("throughput (put ops/sec)", color="#357ABD")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels)
    ax1.set_title("Part A — throughput vs live nodes (fsync on, 100% put)")
    for rect, v in zip(bars, thr):
        ax1.text(rect.get_x() + rect.get_width() / 2, v, f"{v:.0f}", ha="center", va="bottom")

    ax2 = ax1.twinx()
    ax2.plot(list(x), p95, "o-", color="#D9534F", label="p95 latency")
    ax2.set_ylabel("p95 latency (ms)", color="#D9534F")
    ax1.set_xlabel("3 live = minimum quorum (majority of 5)")
    fig.tight_layout()
    out = os.path.join(HERE, "partA_throughput_vs_livenodes.png")
    fig.savefig(out, dpi=120)
    print("wrote", out)


def part_b(rows):
    b = [r for r in rows if r["part"] == "B"]
    b.sort(key=lambda r: int(r["electionTimeoutMs"].split("-")[0]))
    labels = [r["electionTimeoutMs"] + " ms" for r in b]
    fo = [float(r["failoverMs"]) for r in b]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(labels, fo, color=["#5CB85C", "#F0AD4E"], width=0.5)
    ax.set_ylabel("failover time (ms)")
    ax.set_xlabel("election-timeout window")
    ax.set_title("Part B — failover time vs election timeout")
    for rect, v in zip(bars, fo):
        ax.text(rect.get_x() + rect.get_width() / 2, v, f"{v:.0f} ms", ha="center", va="bottom")
    fig.tight_layout()
    out = os.path.join(HERE, "partB_failover_vs_timeout.png")
    fig.savefig(out, dpi=120)
    print("wrote", out)


def part_c(rows):
    c = {r["fsync"]: r for r in rows if r["part"] == "C"}
    metrics = ["avgMs", "p50Ms", "p95Ms", "p99Ms"]
    on = [float(c["on"][m]) for m in metrics]
    off = [float(c["off"][m]) for m in metrics]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = range(len(metrics))
    w = 0.38
    ax.bar([i - w / 2 for i in x], on, width=w, label="fsync on", color="#D9534F")
    ax.bar([i + w / 2 for i in x], off, width=w, label="fsync off", color="#5BC0DE")
    ax.set_yscale("log")
    ax.set_ylabel("latency (ms, log scale)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(["avg", "p50", "p95", "p99"])
    thr_on = float(c["on"]["throughputOpsSec"])
    thr_off = float(c["off"]["throughputOpsSec"])
    ax.set_title(f"Part C — put latency: fsync on vs off\n"
                 f"throughput {thr_on:.0f} vs {thr_off:.0f} ops/s  (~{thr_off/max(thr_on,1):.0f}x)")
    ax.legend()
    for i, (a, b) in enumerate(zip(on, off)):
        ax.text(i - w / 2, a, f"{a:.1f}", ha="center", va="bottom", fontsize=8)
        ax.text(i + w / 2, b, f"{b:.1f}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    out = os.path.join(HERE, "partC_fsync_latency.png")
    fig.savefig(out, dpi=120)
    print("wrote", out)


def main():
    rows = load()
    part_a(rows)
    part_b(rows)
    part_c(rows)


if __name__ == "__main__":
    main()
