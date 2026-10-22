#!/usr/bin/env python
"""Drift of a burst lane in an e-Callisto spectrogram, and the MWA channel that follows it.

For each time bin the frequency of the emission maximum inside a band (background-subtracted,
smoothed) is taken, bins with weak emission are dropped, and f(t) is fitted with a straight
line in log f (constant relative drift, d ln f / dt). For each image step of the MWA
observation the script lists the available MWA coarse channel nearest to the fitted f(t).

    python src/burst_drift.py --inst ASSA_57 --start 2025-12-12T05:08:30 --end 2025-12-12T05:12:40 \
        --obs 2025-12-12T05:09:34 120 --band 140 175 --band 185 220 \
        --channels 97,120,132,138,143,146,152,158,161,167,173,220 --step 2 --out drift_1449551392
"""
import argparse
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ecallisto_mwa_bursts import load


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--inst", default="ASSA_57")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--obs", nargs=2, required=True, metavar=("START", "SECONDS"))
    ap.add_argument("--band", nargs=2, type=float, action="append", required=True, metavar=("FMIN", "FMAX"))
    ap.add_argument("--channels", required=True, help="available MWA coarse channels, comma separated")
    ap.add_argument("--step", type=float, default=2.0, help="image time step, s")
    ap.add_argument("--bin", type=float, default=1.0, help="time bin for the lane maximum, s")
    ap.add_argument("--frac", type=float, default=0.3,
                    help="keep time bins whose band-integrated excess is above this fraction of its maximum")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    t0, t1 = pd.Timestamp(a.start), pd.Timestamp(a.end)
    df = load(a.inst, t0, t1)
    df = df.loc[:, (df.columns >= 108) & (df.columns <= 380)].astype(float)
    ex = df - df.median()
    ex = ex.resample(f"{a.bin}s").mean()
    noise = 1.4826 * (ex - ex.median()).abs().median()
    snr = ex / noise.replace(0, np.nan)
    f = snr.columns.values.astype(float)
    s0 = pd.Timestamp(a.obs[0]); s1 = s0 + pd.Timedelta(seconds=float(a.obs[1]))
    chans = np.array([int(c) for c in a.channels.split(",")])
    res = {"inst": a.inst, "obs": [str(s0), str(s1)], "lanes": []}

    fig, ax = plt.subplots(figsize=(10, 4.2), constrained_layout=True)
    ax.pcolormesh(ex.index, f, ex.T.values, cmap="Blues", vmin=0,
                  vmax=max(5, np.nanpercentile(ex.values, 99.5)), shading="nearest", rasterized=True)
    for c in chans:
        ax.axhline(c * 1.28, color="0.6", lw=0.5, ls=":")
        ax.text(t1, c * 1.28, f" ch{c}", fontsize=7, color="0.4", va="center")
    ax.axvspan(s0, s1, color="#eb6834", alpha=0.12)
    colors = ["#d6262b", "#7b2cbf"]
    for k, (fmin, fmax) in enumerate(a.band):
        m = (f >= fmin) & (f <= fmax)
        # lane position: intensity-weighted centroid frequency of the positive excess in the band
        # (the burst is only a few digits above background, so a peak search on S/N is too noisy)
        sub = ex.loc[:, m].rolling(3, center=True, min_periods=1).median()   # time smoothing
        sub = sub.T.rolling(3, center=True, min_periods=1).median().T          # frequency smoothing
        w = np.clip(sub.values, 0, None)
        strength = w.sum(axis=1)
        fpk = (w * f[m]).sum(axis=1) / np.where(strength > 0, strength, np.nan)
        tt = (sub.index - s0).total_seconds().values
        near = (sub.index >= s0 - pd.Timedelta(seconds=30)) & (sub.index <= s1 + pd.Timedelta(seconds=30))
        ok = strength > a.frac * np.nanmax(strength[near])
        sel = ok & near & np.isfinite(fpk)
        if sel.sum() < 5:
            print(f"band {fmin}-{fmax} MHz: too few points ({sel.sum()})")
            continue
        p = np.polyfit(tt[sel], np.log(fpk[sel]), 1)
        fit = lambda x: np.exp(np.polyval(p, x))
        rel = p[0]                           # d ln f / dt  [1/s]
        drift = rel * fit(np.mean(tt[sel]))  # MHz/s at the mean time
        steps = np.arange(0, float(a.obs[1]) - a.step / 2, a.step)
        plan = []
        for st in steps:
            fc = float(fit(st + a.step / 2))
            ch = int(chans[np.argmin(np.abs(chans * 1.28 - fc))])
            plan.append(dict(t0=str(s0 + pd.Timedelta(seconds=float(st))), t1=str(s0 + pd.Timedelta(seconds=float(st + a.step))),
                             f_drift=round(fc, 2), ch=ch, f_ch=round(ch * 1.28, 2), df=round(ch * 1.28 - fc, 2)))
        res["lanes"].append(dict(band=[fmin, fmax], n_points=int(sel.sum()), dlnf_dt=float(rel), drift_mhz_s=float(drift),
                                 f_at_obs_start=float(fit(0)), f_at_obs_end=float(fit(float(a.obs[1]))), plan=plan))
        ax.plot(sub.index[sel], fpk[sel], ".", ms=3, color=colors[k % 2])
        xx = np.linspace(tt[sel].min(), tt[sel].max(), 100)
        ax.plot(s0 + pd.to_timedelta(xx, unit="s"), fit(xx), "-", color=colors[k % 2], lw=1.5,
                label=f"{fmin:.0f}-{fmax:.0f} MHz: {drift:+.3f} MHz/s")
        print(f"band {fmin}-{fmax} MHz: {sel.sum()} points, drift {drift:+.3f} MHz/s (d ln f/dt {rel:+.2e} /s), "
              f"f = {fit(0):.1f} -> {fit(float(a.obs[1])):.1f} MHz over the observation")
        used = pd.Series([q["ch"] for q in plan]).value_counts().to_dict()
        print("   channels used for", a.step, "s steps:", used)
    ax.set_ylim(min(b[0] for b in a.band) - 30, max(b[1] for b in a.band) + 30)
    ax.invert_yaxis()
    ax.set_xlim(t0, t1)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.set_ylabel("MHz")
    ax.set_title(f"e-Callisto {a.inst}: lane maxima (dots) and drift fit (lines); orange: MWA observation; "
                 f"dotted: available MWA channels", fontsize=8.5, loc="left")
    ax.legend(fontsize=8, loc="lower left")
    fig.savefig(a.out + ".png", dpi=110)
    json.dump(res, open(a.out + ".json", "w"), indent=1)
    print("wrote", a.out + ".png", a.out + ".json")


if __name__ == "__main__":
    main()
