#!/usr/bin/env python
"""e-Callisto dynamic spectrum per event of the MWA image board.

Reads the board's board.json (solar-burst-multiview scripts/calculon/export_board.py) and
writes, for each event, OUTDIR/<obsid>_spectrum.png: Australia-ASSA spectrogram
(ASSA_57, or ASSA_60 before December 2023; background subtracted, 108-380 MHz) from
5 min before to 5 min after the MWA observation, with the observation shaded (orange),
each image product's window and channel marked (red), and GOES flare peaks (dashed).
Per product also OUTDIR/<product>_spectrum.png: the MWA observation only, with one box per
image of that product (its start time to start + time resolution, its 1.28 MHz coarse channel),
so a movie's frames can be found in the spectrum. DATE-OBS of a P-AIRCARS/WSClean image is
the start of its interval (the first frames of 1449551392 start at 05:09:39.4 + 2 s steps, at
the MS start). Remade when the product's frame count changes.

    python src/board_spectra.py BOARD_DIR/board.json --outdir BOARD_DIR/img
"""
import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

from ecallisto_mwa_bursts import load


def product_figure(df, inst, e, p, out):
    """One product's images as boxes in time and frequency over the e-Callisto spectrum."""
    s0, s1 = pd.Timestamp(e["obs_start"]), pd.Timestamp(e["obs_end"])
    tres = float(p.get("tres") or 2)
    fr = p.get("frames") or []
    fq = [f["freq_mhz"] for f in fr] or [float(str(p["ch"]).split(">")[0]) * 1.28]
    f0, f1 = max(108, min(fq) - 25), min(380, max(fq) + 25)
    fig, ax = plt.subplots(figsize=(10, 4.2), constrained_layout=True)
    if df is not None and len(df) > 20:
        sub = df.loc[(df.index >= s0 - pd.Timedelta(seconds=20)) & (df.index <= s1 + pd.Timedelta(seconds=20))]
        sub = sub.loc[:, (sub.columns >= f0) & (sub.columns <= f1)]
        ex = df.loc[:, sub.columns].median()
        ex = sub - ex
        if len(ex):
            ax.pcolormesh(ex.index, ex.columns, ex.T.values, cmap="Blues", vmin=0,
                          vmax=max(5, np.nanpercentile(ex.values, 99.7)), shading="nearest", rasterized=True)
    else:
        ax.text(0.5, 0.5, f"no {inst} data", transform=ax.transAxes, ha="center")
    ax.axvspan(s0, s1, color="#eb6834", alpha=0.08)
    w = tres / 86400
    for f in fr:
        t = mdates.date2num(pd.Timestamp(f["t"]))
        ax.add_patch(Rectangle((t, f["freq_mhz"] - 0.64), w, 1.28, fill=False, ec="#d6262b", lw=0.8))
    if not fr and p.get("trange"):   # no images yet: the requested window and channel, dashed
        a, b = (pd.Timestamp(x.replace("/", "-", 2).replace("/", "T")) for x in p["trange"].split("~"))
        ax.add_patch(Rectangle((mdates.date2num(a), fq[0] - 0.64), mdates.date2num(b) - mdates.date2num(a), 1.28,
                               fill=False, ec="#d6262b", lw=0.8, ls="--"))
    ax.set_ylim(f1, f0)
    ax.set_xlim(s0 - pd.Timedelta(seconds=15), s1 + pd.Timedelta(seconds=15))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.set_ylabel("MHz")
    ax.set_xlabel(f"UTC {e['date']}")
    ax.set_title(f"e-Callisto Australia-{inst}, background subtracted; MWA {e['sun']} {p['label']}: "
                 f"{len(fr)} images, one red box each ({tres:g} s × 1.28 MHz channel); orange: MWA observation", fontsize=8.5, loc="left")
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("board")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--pad", type=float, default=5.0, help="minutes before and after the observation")
    ap.add_argument("--force", action="store_true", help="remake existing figures")
    a = ap.parse_args()
    b = json.load(open(a.board))
    os.makedirs(a.outdir, exist_ok=True)
    for e in b["events"]:
        if not e.get("obs_start"):
            continue
        out = os.path.join(a.outdir, f"{e['sun']}_spectrum.png")
        prods = [p for p in b["products"] if p["sun"] == e["sun"]]
        todo = []   # per-product figures whose frame count changed
        for p in prods:
            stamp = os.path.join(a.outdir, f"{p['product']}_spectrum.stamp")
            n = str(len(p.get("frames") or []))
            if a.force or not os.path.exists(stamp) or open(stamp).read() != n:
                todo.append((p, stamp, n))
        if os.path.exists(out) and not a.force and not todo:
            continue
        s0, s1 = pd.Timestamp(e["obs_start"]), pd.Timestamp(e["obs_end"])
        t0, t1 = s0 - pd.Timedelta(minutes=a.pad), s1 + pd.Timedelta(minutes=a.pad)
        inst = "ASSA_57" if s0 >= pd.Timestamp("2023-12-01") else "ASSA_60"
        df = load(inst, t0, t1)
        for p, stamp, n in todo:
            product_figure(df, inst, e, p, os.path.join(a.outdir, f"{p['product']}_spectrum.png"))
            open(stamp, "w").write(n)
        if os.path.exists(out) and not a.force:
            continue
        fig, ax = plt.subplots(figsize=(10, 3.6), constrained_layout=True)
        if df is not None and len(df) > 20:
            df = df.loc[:, (df.columns >= 108) & (df.columns <= 380)].astype(float)
            ex = df - df.median()
            ax.pcolormesh(ex.index, ex.columns, ex.T.values, cmap="Blues", vmin=0,
                          vmax=max(5, np.nanpercentile(ex.values, 99.7)), shading="nearest", rasterized=True)
        else:
            ax.text(0.5, 0.5, f"no {inst} data", transform=ax.transAxes, ha="center")
        ax.axvspan(s0, s1, color="#eb6834", alpha=0.12, label="MWA observation")
        for p in prods:
            w = (p.get("trange") or "").split("~")
            if len(w) == 2:
                w0, w1 = (pd.Timestamp(x.replace("/", "-", 2).replace("/", "T")) for x in w)
                ax.axvspan(w0, w1 + pd.Timedelta(seconds=0.5), ymin=0, ymax=1, color="#d6262b", alpha=0.5)
            f = (p["frames"][0]["freq_mhz"] if p.get("frames") else float(p["ch"]) * 1.28)
            ax.axhline(f, color="#d6262b", lw=0.8, ls=":")
            ax.text(t1, f, f" ch{p['ch']}", color="#d6262b", fontsize=7, va="center")
        for g in e.get("goes") or []:
            ax.axvline(pd.Timestamp(g["peak"]), color="0.3", lw=0.8, ls="--")
            ax.text(pd.Timestamp(g["peak"]), 112, f" {g['cls']}", fontsize=7, color="0.2", va="top")
        ax.set_ylim(380, 108)
        ax.set_xlim(t0, t1)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.set_ylabel("MHz")
        ax.set_xlabel(f"UTC {e['date']}")
        ax.set_title(f"e-Callisto Australia-{inst}, background subtracted; orange: MWA {e['sun']}; "
                     f"red: image windows and channels; dashed: GOES peaks", fontsize=8.5, loc="left")
        fig.savefig(out, dpi=100)
        plt.close(fig)
        print(out)


if __name__ == "__main__":
    main()
