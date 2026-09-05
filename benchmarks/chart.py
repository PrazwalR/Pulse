#!/usr/bin/env python3
"""Render the E1/E2 benchmark CSVs to standalone SVG charts (no dependencies).

Reads benchmarks/data/*.csv and writes benchmarks/charts/*.svg.
Run:  python3 benchmarks/chart.py
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
CHARTS = os.path.join(HERE, "charts")

BG, INK, MUTE, GRID = "#ffffff", "#1a1a1a", "#6b7280", "#e5e7eb"
OK, FAIL, BAR = "#2a9d5c", "#e23b3b", "#2f6fed"
BAND, BAND_LINE = "#fdeaea", "#f2b8b8"
FONT = "font-family='ui-sans-serif,-apple-system,Segoe UI,Roboto,sans-serif'"


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def read_csv(name):
    with open(os.path.join(DATA, name)) as f:
        return list(csv.DictReader(f))


def txt(x, y, s, size=12, anchor="start", fill=INK, weight="normal"):
    return (f"<text x='{x:.1f}' y='{y:.1f}' font-size='{size}' text-anchor='{anchor}' "
            f"fill='{fill}' font-weight='{weight}' {FONT}>{esc(s)}</text>")


def line(x1, y1, x2, y2, stroke, w=1, dash=None):
    d = f" stroke-dasharray='{dash}'" if dash else ""
    return f"<line x1='{x1:.1f}' y1='{y1:.1f}' x2='{x2:.1f}' y2='{y2:.1f}' stroke='{stroke}' stroke-width='{w}'{d}/>"


def rect(x, y, w, h, fill, rx=0):
    return f"<rect x='{x:.1f}' y='{y:.1f}' width='{w:.1f}' height='{h:.1f}' rx='{rx}' fill='{fill}'/>"


def polyline(pts, stroke, w=2):
    p = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    return f"<polyline points='{p}' fill='none' stroke='{stroke}' stroke-width='{w}' stroke-linejoin='round'/>"


def svg_doc(w, h, body, title):
    return (f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' viewBox='0 0 {w} {h}'>"
            f"<rect width='{w}' height='{h}' fill='{BG}'/>"
            f"{txt(20, 28, title, 16, 'start', INK, 'bold')}{body}</svg>")


def e1_chart():
    rows = read_csv("e1_consistency.csv")
    W, H, L, R, T, B = 720, 430, 70, 30, 70, 70
    pw, ph = W - L - R, H - T - B
    maxv = max(float(r["writes_per_sec"]) for r in rows) * 1.15
    body = [txt(L - 8, T - 16, "writes/sec", 11, "end", MUTE)]
    for i in range(6):
        v, y = maxv * i / 5, T + ph - ph * i / 5
        body += [line(L, y, W - R, y, GRID), txt(L - 8, y + 4, f"{int(v)}", 11, "end", MUTE)]
    slot = pw / len(rows)
    for i, r in enumerate(rows):
        v = float(r["writes_per_sec"])
        bw = slot * 0.46
        x = L + slot * i + (slot - bw) / 2
        bh = ph * v / maxv
        y = T + ph - bh
        surv = r["survives_node_down"] == "yes"
        body += [
            rect(x, y, bw, bh, BAR, 4),
            txt(x + bw / 2, y - 8, f"{int(v)}", 14, "middle", INK, "bold"),
            txt(x + bw / 2, T + ph + 20, r["cl"], 14, "middle", INK, "bold"),
            txt(x + bw / 2, T + ph + 38, f"p99 {r['p99_ms']} ms", 11, "middle", MUTE),
            txt(x + bw / 2, T + ph + 54, "survives 1 node down" if surv else "fails on 1 node down",
                10, "middle", OK if surv else FAIL),
        ]
    body.append(line(L, T + ph, W - R, T + ph, MUTE))
    return svg_doc(W, H, "".join(body), "E1 — Write throughput vs consistency level (3-node, RF 3)")


def _panel(rows, x0, y0, pw, ph, title, kill_t, restart_t):
    ts = [int(r["elapsed_sec"]) for r in rows]
    oks = [int(r["emitted_per_sec"]) for r in rows]
    fails = [int(r["failed_per_sec"]) for r in rows]
    maxt = max(ts)
    maxv = max(max(oks), max(fails), 1) * 1.2
    X = lambda t: x0 + pw * t / maxt
    Y = lambda v: y0 + ph - ph * v / maxv
    body = [txt(x0, y0 - 10, title, 13, "start", INK, "bold")]
    if kill_t is not None:
        rt = restart_t if restart_t is not None else maxt
        body += [rect(X(kill_t), y0, X(rt) - X(kill_t), ph, BAND),
                 line(X(kill_t), y0, X(kill_t), y0 + ph, BAND_LINE, 1, "4 3"),
                 txt(X(kill_t) + 5, y0 + 14, "cass3 down", 10, "start", FAIL)]
    for i in range(6):
        v, y = maxv * i / 5, y0 + ph - ph * i / 5
        body += [line(x0, y, x0 + pw, y, GRID), txt(x0 - 8, y + 4, f"{int(v)}", 10, "end", MUTE)]
    for t in range(0, maxt + 1, 10):
        body.append(txt(X(t), y0 + ph + 16, str(t), 10, "middle", MUTE))
    body += [line(x0, y0 + ph, x0 + pw, y0 + ph, MUTE),
             polyline([(X(t), Y(v)) for t, v in zip(ts, oks)], OK, 2.2),
             polyline([(X(t), Y(v)) for t, v in zip(ts, fails)], FAIL, 2.2)]
    return "".join(body)


def e2_chart():
    q, a = read_csv("e2_quorum.csv"), read_csv("e2_all.csv")
    W, H, L, ph = 780, 580, 70, 175
    pw = W - L - 30
    body = [_panel(q, L, 72, pw, ph, "QUORUM — stays available (failed writes = 0)", 25, 70),
            _panel(a, L, 330, pw, ph, "ALL — loses availability while a replica is down", 19, 49)]
    ly = H - 16
    body += [rect(L, ly - 10, 16, 4, OK), txt(L + 22, ly, "writes/sec (ok)", 11, "start", INK),
             rect(L + 150, ly - 10, 16, 4, FAIL), txt(L + 172, ly, "failed/sec", 11, "start", INK),
             txt(W - 30, ly, "seconds", 11, "end", MUTE)]
    return svg_doc(W, H, "".join(body), "E2 — Node-kill / CAP: the consistency knob decides availability")


def main():
    os.makedirs(CHARTS, exist_ok=True)
    for name, svg in (("e1_consistency.svg", e1_chart()), ("e2_node_kill.svg", e2_chart())):
        path = os.path.join(CHARTS, name)
        with open(path, "w") as f:
            f.write(svg)
        print("wrote", os.path.relpath(path, os.path.dirname(HERE)))


if __name__ == "__main__":
    main()
