#!/usr/bin/env python3

import os
import re
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("ERROR: matplotlib not found. Install it:")
    print("  pip3 install matplotlib")
    sys.exit(1)

RESULTS_DIR = Path(__file__).parent / "results"
PLOTS_DIR = RESULTS_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

BACKENDS = ["mem", "elasticsearch", "clickhouse", "cassandra", "mysql"]
BACKEND_LABELS = {
    "mem":           "In-Memory",
    "elasticsearch": "Elasticsearch",
    "clickhouse":    "ClickHouse",
    "cassandra":     "Cassandra",
    "mysql":         "MySQL",
}
COLORS = {
    "mem":           "#4e79a7",
    "elasticsearch": "#f28e2b",
    "clickhouse":    "#e15759",
    "cassandra":     "#76b7b2",
    "mysql":         "#59a14f",
}

WRITE_RATES = [200, 500, 1000, 2000]
READ_RATES  = [50, 100, 200]


# ---------------------------------------------------------------------------
def parse_file(path: Path) -> dict:
    """Return dict with p50_ms, p99_ms, rps extracted from a wrk2 output file."""
    if not path.exists():
        return {}
    text = path.read_text()

    result = {}

    # Latency percentiles  (e.g. "  50.000%    4.50ms")
    for pct, key in [("50.000", "p50_ms"), ("99.000", "p99_ms")]:
        m = re.search(rf"{pct}%\s+([\d.]+)(ms|us|s)", text)
        if m:
            val, unit = float(m.group(1)), m.group(2)
            if unit == "us":
                val /= 1000.0
            elif unit == "s":
                val *= 1000.0
            result[key] = val

    # Requests/sec
    m = re.search(r"Requests/sec:\s+([\d.]+)", text)
    if m:
        result["rps"] = float(m.group(1))

    return result


def load_all() -> dict:
    """Load all result files into a nested dict: data[backend][test_key] = metrics."""
    data = {}
    for backend in BACKENDS:
        bdir = RESULTS_DIR / backend
        if not bdir.exists():
            continue
        data[backend] = {}
        for rate in WRITE_RATES:
            f = bdir / f"write_{rate}rps.txt"
            parsed = parse_file(f)
            if parsed:
                data[backend][f"write_{rate}"] = parsed
        for rate in READ_RATES:
            f = bdir / f"read_{rate}rps.txt"
            parsed = parse_file(f)
            if parsed:
                data[backend][f"read_{rate}"] = parsed
    return data


# ---------------------------------------------------------------------------
def plot_write_latency(data: dict):
    """Line chart: write p99 latency vs target rate, one line per backend."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Write Latency vs Target Rate", fontsize=14, fontweight="bold")

    for ax, metric, ylabel, title in [
        (axes[0], "p50_ms", "Latency, ms", "P50 (median)"),
        (axes[1], "p99_ms", "Latency, ms", "P99"),
    ]:
        for backend in BACKENDS:
            if backend not in data:
                continue
            xs, ys = [], []
            for rate in WRITE_RATES:
                key = f"write_{rate}"
                v = data[backend].get(key, {}).get(metric)
                if v is not None:
                    xs.append(rate)
                    ys.append(v)
            if xs:
                ax.plot(xs, ys, marker="o", label=BACKEND_LABELS[backend],
                        color=COLORS[backend], linewidth=2, markersize=6)

        ax.set_xlabel("Target rate, rps")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xticks(WRITE_RATES)
        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.5)

    fig.tight_layout()
    out = PLOTS_DIR / "write_latency.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def plot_write_throughput(data: dict):
    """Bar chart: actual throughput (rps) per backend at each target rate."""
    available_backends = [b for b in BACKENDS if b in data]
    if not available_backends:
        return

    n_backends = len(available_backends)
    n_rates = len(WRITE_RATES)
    bar_width = 0.8 / n_backends
    x = np.arange(n_rates)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.set_title("Write Throughput (actual rps)", fontsize=14, fontweight="bold")

    for i, backend in enumerate(available_backends):
        heights = []
        for rate in WRITE_RATES:
            v = data[backend].get(f"write_{rate}", {}).get("rps")
            heights.append(v if v is not None else 0)

        offset = (i - n_backends / 2 + 0.5) * bar_width
        bars = ax.bar(x + offset, heights, bar_width,
                      label=BACKEND_LABELS[backend],
                      color=COLORS[backend], alpha=0.85)
        for bar, h in zip(bars, heights):
            if h > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 5,
                        f"{h:.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_xlabel("Target rate, rps")
    ax.set_ylabel("Actual throughput, rps")
    ax.set_xticks(x)
    ax.set_xticklabels(WRITE_RATES)
    ax.legend()
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()

    out = PLOTS_DIR / "write_throughput.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def plot_read_latency(data: dict):
    """Grouped bar chart: read p50 and p99 per backend at each target rate."""
    available_backends = [b for b in BACKENDS if b in data]
    if not available_backends:
        return

    n_backends = len(available_backends)
    bar_width = 0.35 / n_backends
    x = np.arange(len(READ_RATES))

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Read Latency (/api/v2/traces)", fontsize=14, fontweight="bold")

    for ax, metric, title in [
        (axes[0], "p50_ms", "P50 (median)"),
        (axes[1], "p99_ms", "P99"),
    ]:
        for i, backend in enumerate(available_backends):
            heights = []
            for rate in READ_RATES:
                v = data[backend].get(f"read_{rate}", {}).get(metric)
                heights.append(v if v is not None else 0)

            offset = (i - n_backends / 2 + 0.5) * bar_width * 2
            bars = ax.bar(x + offset, heights, bar_width * 2,
                          label=BACKEND_LABELS[backend],
                          color=COLORS[backend], alpha=0.85)
            for bar, h in zip(bars, heights):
                if h > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, h + 0.3,
                            f"{h:.1f}", ha="center", va="bottom", fontsize=7)

        ax.set_xlabel("Target rate, rps")
        ax.set_ylabel("Latency, ms")
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(READ_RATES)
        ax.legend()
        ax.grid(True, axis="y", linestyle="--", alpha=0.5)

    fig.tight_layout()
    out = PLOTS_DIR / "read_latency.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
def print_summary(data: dict):
    print("\n=== Summary ===")
    for backend in BACKENDS:
        if backend not in data:
            continue
        print(f"\n{BACKEND_LABELS[backend]}:")
        for rate in WRITE_RATES:
            m = data[backend].get(f"write_{rate}", {})
            if m:
                print(f"  write {rate:4d} rps → "
                      f"p50={m.get('p50_ms', '?'):7.2f}ms  "
                      f"p99={m.get('p99_ms', '?'):7.2f}ms  "
                      f"actual={m.get('rps', '?'):7.1f} rps")
        for rate in READ_RATES:
            m = data[backend].get(f"read_{rate}", {})
            if m:
                print(f"  read  {rate:4d} rps → "
                      f"p50={m.get('p50_ms', '?'):7.2f}ms  "
                      f"p99={m.get('p99_ms', '?'):7.2f}ms")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    data = load_all()

    if not data:
        print(f"No result files found in {RESULTS_DIR}")
        print("Run run_benchmark.sh for each backend first.")
        sys.exit(1)

    print_summary(data)
    plot_write_latency(data)
    plot_write_throughput(data)
    plot_read_latency(data)

    print(f"\nAll charts saved to: {PLOTS_DIR}")
