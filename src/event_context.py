#!/usr/bin/env python
"""Context light curves of one MWA burst event, all at Earth time on one axis:
GOES XRS (1-8 and 0.5-4 A), STIX quick-look counts (light-time corrected to Earth,
stixdcpy), an e-Callisto spectrogram (background subtracted) and, if given, the MWA
short-baseline light curve of one coarse channel (npz from solarburst.dynspec).
The MWA observation is shaded, the MWA image interval marked.

    python src/event_context.py --start 2024-12-25T04:10 --end 2024-12-25T05:10 \
        --mwa-obs 2024-12-25T04:47:58 240 --image 2024-12-25T04:51:45.5 4 \
        --station ASSA_57 --mwa-npz dynspec_ch127.npz --out context.png
"""
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ecallisto_mwa_bursts import load


def goes(t0, t1):
    from sunpy import timeseries as ts
    from sunpy.net import Fido
    from sunpy.net import attrs as a
    res = Fido.search(a.Time(t0, t1), a.Instrument("XRS"), a.Resolution("flx1s"))
    sats = sorted({int(s) for s in res[0]["SatelliteNumber"]}) if len(res[0]) else []
    for sat in ([16, 18] + sats):          # primary GOES first
        r = res[0][res[0]["SatelliteNumber"] == sat]
        if len(r):
            files = Fido.fetch(r)
            df = ts.TimeSeries(files, concatenate=True).to_dataframe()
            return sat, df[(df.index >= t0) & (df.index <= t1)]
    return None, None


def stix(t0, t1):
    from stixdcpy.quicklook import LightCurves
    d = LightCurves.from_sdc(start_utc=t0.isoformat(), end_utc=t1.isoformat(), ltc=True).data
    t = pd.to_datetime(d["start_unix"] + np.asarray(d["delta_time"]), unit="s")
    return t, np.asarray(d["counts"], float), d["energy_bins"]["names"], np.asarray(d["rcr"])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--mwa-obs", nargs=2, metavar=("START", "SECONDS"))
    ap.add_argument("--image", nargs=2, metavar=("START", "SECONDS"))
    ap.add_argument("--station", default="ASSA_57", help="key of ecallisto_mwa_bursts.INSTS")
    ap.add_argument("--mwa-npz")
    ap.add_argument("--title", default="")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    t0, t1 = pd.Timestamp(a.start), pd.Timestamp(a.end)

    rows = 4 if a.mwa_npz else 3
    fig, axs = plt.subplots(rows, 1, figsize=(11, 2.6 * rows), sharex=True, constrained_layout=True,
                            gridspec_kw=dict(height_ratios=[1, 1, 1.4] + ([0.8] if a.mwa_npz else [])))
    sat, g = goes(t0, t1)
    ax = axs[0]
    if g is not None:
        ax.semilogy(g.index, g["xrsb"], color="#c4511d", lw=1, label="1–8 Å")
        ax.semilogy(g.index, g["xrsa"], color="#1c5cab", lw=1, label="0.5–4 Å")
        for lev, cls in zip([1e-6, 1e-5, 1e-4], "CMX"):
            ax.axhline(lev, color="0.6", lw=0.5, ls=":")
            ax.text(t1, lev, f" {cls}", va="center", fontsize=8, color="0.4")
        ax.set_ylabel("W m⁻²")
        ax.set_title(f"GOES-{sat} XRS (1 s)", loc="left", fontsize=9)
        ax.legend(fontsize=8, loc="upper left")

    ts_, counts, names, rcr = stix(t0, t1)
    ax = axs[1]
    cols = ["#d6262b", "#e8891c", "#2a9d4a", "#1c5cab", "#6a3d9a"]
    for c, n, col in zip(counts, names, cols):
        ax.semilogy(ts_, np.where(c > 0, c, np.nan), lw=0.9, color=col, label=n)
    if (rcr > 0).any():
        ax.fill_between(ts_, 0, 1, where=rcr > 0, transform=ax.get_xaxis_transform(), color="0.85", zorder=0,
                        label="attenuator / rate control")
    ax.set_ylabel("counts / 4 s")
    ax.set_title("STIX quick-look light curves, light-time corrected to Earth", loc="left", fontsize=9)
    ax.legend(fontsize=7, ncol=3, loc="upper left")

    ax = axs[2]
    df = load(a.station, t0, t1)
    if df is not None:
        df = df.loc[:, (df.columns >= 108) & (df.columns <= 380)].astype(float)
        ex = df - df.median()
        ax.pcolormesh(ex.index, ex.columns, ex.T.values, cmap="Blues", vmin=0,
                      vmax=max(5, np.nanpercentile(ex.values, 99.7)), shading="nearest", rasterized=True)
        ax.set_ylim(380, 108)
    ax.set_ylabel("MHz")
    ax.set_title(f"e-Callisto Australia-{a.station}, background subtracted", loc="left", fontsize=9)

    if a.mwa_npz:
        d = np.load(a.mwa_npz)
        tm = pd.to_datetime(d["unix"], unit="s")
        with np.errstate(all="ignore"):
            lc = np.nanmedian(d["amp"] / np.nanmedian(d["amp"], axis=1, keepdims=True), axis=0)
        ax = axs[3]
        ax.plot(tm, lc, lw=0.7, color="#0d366b")
        ax.set_ylabel("amp / median")
        ax.set_title(f"MWA short baselines, {d['freq_mhz'].min():.1f}–{d['freq_mhz'].max():.1f} MHz", loc="left",
                     fontsize=9)

    for i, ax in enumerate(axs):
        if a.mwa_obs:
            s0 = pd.Timestamp(a.mwa_obs[0])
            ax.axvspan(s0, s0 + pd.Timedelta(seconds=float(a.mwa_obs[1])), color="#eb6834", alpha=0.15,
                       label="MWA obs" if i == 0 else None)
        if a.image:
            s0 = pd.Timestamp(a.image[0])
            ax.axvspan(s0, s0 + pd.Timedelta(seconds=float(a.image[1])), color="#d6262b", alpha=0.6,
                       label="MWA image" if i == 0 else None)
    axs[0].legend(fontsize=8, loc="upper left")
    axs[-1].set_xlim(t0, t1)
    axs[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    axs[-1].set_xlabel(f"UTC on {t0:%Y-%m-%d} (Earth time)")
    if a.title:
        fig.suptitle(a.title, x=0.01, ha="left", fontsize=10)
    fig.savefig(a.out, dpi=100)
    print(a.out)


if __name__ == "__main__":
    main()
