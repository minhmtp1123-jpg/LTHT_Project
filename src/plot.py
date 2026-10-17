#!/usr/bin/env python3
"""Ve bieu do tu ket qua cua bench.

Dung:  python3 plot.py results.csv [thu_muc_dau_ra]
Tao:   latency.png, throughput.png
Cai:   sudo pacman -S python-matplotlib
"""
import csv
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")           # khong can man hinh
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

ORDER = ["pipe", "unix_stream", "unix_seqp", "unix_dgram", "tcp"]
STYLE = {                        # mau + marker khac nhau de in den-trang van doc duoc
    "pipe":        ("#1b9e77", "o"),
    "unix_stream": ("#d95f02", "s"),
    "unix_seqp":   ("#7570b3", "^"),
    "unix_dgram":  ("#e7298a", "D"),
    "tcp":         ("#386cb0", "v"),
}


def load(path):
    data = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            data[row["mech"]].append((
                int(row["size"]),
                float(row["lat_mean_us"]),
                float(row["lat_p50_us"]),
                float(row["lat_p99_us"]),
                float(row["throughput_MBps"]),
            ))
    for k in data:
        data[k].sort()
    return data


def fmt_size(b):
    return f"{b // 1024}K" if b >= 1024 else str(b)


def nice_ticks(lo, hi):
    """Moc 1-2-5 x 10^k bao phu [lo, hi] cho truc log."""
    cand = [m * 10 ** k for k in range(-1, 7) for m in (1, 2, 5)]
    below = [c for c in cand if c <= lo]
    above = [c for c in cand if c >= hi]
    a, b = (below[-1] if below else cand[0]), (above[0] if above else cand[-1])
    return [c for c in cand if a <= c <= b]


def style_axes(ax, title, ylabel, sizes, yticks):
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(sizes)
    ax.set_xticklabels([fmt_size(s) for s in sizes])
    ax.minorticks_off()
    ax.set_yticks(yticks)                       # nhan so thuong, khong phai 10^n
    ax.yaxis.set_major_formatter(ScalarFormatter())
    ax.set_ylim(yticks[0], yticks[-1])
    ax.set_xlabel("Kích thước thông điệp (byte)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, which="major", alpha=0.3)
    ax.legend()


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    data = load(sys.argv[1])
    out = sys.argv[2].rstrip("/") + "/" if len(sys.argv) > 2 else ""
    mechs = [m for m in ORDER if m in data]
    sizes = sorted({r[0] for m in mechs for r in data[m]})

    # --- do tre: duong = p50, vung to = p50..p99 ---
    fig, ax = plt.subplots(figsize=(8, 5))
    for m in mechs:
        c, mk = STYLE[m]
        xs = [r[0] for r in data[m]]
        p50 = [r[2] for r in data[m]]
        p99 = [r[3] for r in data[m]]
        ax.plot(xs, p50, marker=mk, color=c, label=m)
        ax.fill_between(xs, p50, p99, color=c, alpha=0.12)
    style_axes(ax, "Độ trễ một chiều (p50, vùng mờ đến p99)",
               "Độ trễ (µs, thang log)", sizes, nice_ticks(
                   min(r[2] for m in mechs for r in data[m]),
                   max(r[3] for m in mechs for r in data[m])))
    fig.tight_layout()
    fig.savefig(out + "latency.png", dpi=150)

    # --- thong luong ---
    fig, ax = plt.subplots(figsize=(8, 5))
    for m in mechs:
        c, mk = STYLE[m]
        ax.plot([r[0] for r in data[m]], [r[4] for r in data[m]],
                marker=mk, color=c, label=m)
    tp = [r[4] for m in mechs for r in data[m]]
    style_axes(ax, "Thông lượng một chiều", "Thông lượng (MB/s, thang log)",
               sizes, nice_ticks(min(tp), max(tp)))
    fig.tight_layout()
    fig.savefig(out + "throughput.png", dpi=150)

    print(f"Da tao {out}latency.png va {out}throughput.png")


if __name__ == "__main__":
    main()