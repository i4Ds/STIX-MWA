"""
Combined radio dynamic spectrum of one flare: e-Callisto stations and, if given, MWA.

Top panel: all instruments on one log-frequency axis, each scaled to its own
percentiles and drawn in the order given (later ones on top). Below: one row
per instrument on its own scale. Times are at Earth.

    python src/flare_spectrogram.py files/ecallisto/20220930 \
        --start 2022-09-30T03:28 --end 2022-09-30T04:35 \
        --station Australia-ASSA_63 --station INDIA-OOTY_02 --station Australia-ASSA_60 \
        --mwa dynspec_flare20220930.npz --mwa-obs 1348544016 1348546976 \
        --event "STIX start:2022-09-30T03:33:48" --span "C5.3 AR13110:2022-09-30T04:26:2022-09-30T04:30" \
        --out _results/plots/flare_20220930_spectrogram.png
"""
import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from astropy.time import Time
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from ecallisto_spectrogram import Spectrogram, by_station, stack

MWA_OBS_SECONDS = 296
EVENT_COLOURS = ("cyan", "white", "lime", "orange")
SPAN_COLOURS = ("lime", "orange")


def scaled(img, pct=(5, 99.5)):
    lo, hi = np.nanpercentile(img, pct)
    return np.clip((img - lo) / (hi - lo), 0, 1)


def mwa_spectrogram(path: Path, start: Time, end: Time) -> Spectrogram:
    d = np.load(path)
    time = (d["unix"] * 1e3).astype("datetime64[ms]")
    keep = (time >= start.datetime64) & (time <= end.datetime64)
    with np.errstate(all="ignore"):
        rel = np.log10(d["amp"][:, keep] / np.nanmedian(d["amp"][:, keep], axis=1, keepdims=True))
    return Spectrogram("MWA", rel, d["freq_mhz"], time[keep])


def draw(ax, spec, img, cmap="inferno"):
    t = mdates.date2num(spec.time.astype(object))
    return ax.pcolormesh(t, spec.freq, img, shading="nearest", cmap=cmap, vmin=0, vmax=1, rasterized=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("ecallisto_dir", type=Path)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--station", action="append", default=[], help="STATION_FOCUS, drawing order")
    p.add_argument("--mwa", type=Path, help="npz from solarburst.dynspec")
    p.add_argument("--mwa-obs", nargs="+", type=int, default=[], help="obsids (GPS s) to mark as coverage")
    p.add_argument("--event", action="append", default=[], metavar="LABEL:ISOTIME")
    p.add_argument("--span", action="append", default=[], metavar="LABEL:ISOSTART:ISOEND")
    p.add_argument("--title", default="")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    start, end = Time(args.start), Time(args.end)

    groups = by_station(args.ecallisto_dir)
    specs = []
    for key in args.station:
        s = stack(groups[key], start, end)
        if s is not None:
            s.station = key
            specs.append((s, scaled(s.background_subtracted())))
    if args.mwa and args.mwa.exists():
        m = mwa_spectrogram(args.mwa, start, end)
        specs.append((m, scaled(m.data, (2, 99.8))))

    fig, axs = plt.subplots(1 + len(specs), 1, figsize=(14, 3.6 + 2.0 * len(specs)), sharex=True,
                            gridspec_kw={"height_ratios": [2.2] + [1] * len(specs)})
    top = axs[0]
    for spec, img in specs:
        draw(top, spec, img)
    top.set_yscale("log")
    fmin = min(s.freq.min() for s, _ in specs)
    fmax = max(s.freq.max() for s, _ in specs)
    top.set_ylim(fmin, fmax)
    top.set_yticks([20, 30, 50, 80, 100, 150, 200, 300, 500])
    top.get_yaxis().set_major_formatter(plt.ScalarFormatter())
    top.get_yaxis().set_minor_formatter(plt.NullFormatter())
    top.set_ylabel("MHz")
    top.set_title(args.title or "combined, each instrument scaled to its own percentiles", loc="left", fontsize=10)
    for ax, (spec, img) in zip(axs[1:], specs):
        draw(ax, spec, img)
        ax.set_ylim(spec.freq.min(), spec.freq.max())
        ax.set_ylabel("MHz")
        ax.set_title(f"{spec.station}  {spec.freq.min():.0f}–{spec.freq.max():.0f} MHz", loc="left", fontsize=9)

    handles = []
    for obs in args.mwa_obs:
        t0 = Time(obs, format="gps").utc
        for ax in axs:
            ax.axvspan(mdates.date2num(t0.datetime), mdates.date2num((t0 + MWA_OBS_SECONDS / 86400).datetime),
                       ymin=0.96, ymax=1.0, color="deepskyblue", lw=0)
    if args.mwa_obs:
        handles.append(Patch(color="deepskyblue", label=f"MWA obs ({len(args.mwa_obs)} × {MWA_OBS_SECONDS} s)"))
    for item, colour in zip(args.span, SPAN_COLOURS):
        label, rest = item.split(":", 1)
        s0, s1 = rest[:19], rest[20:]
        x0, x1 = (mdates.date2num(Time(x).datetime) for x in (s0, s1))
        for ax in axs:
            ax.axvspan(x0, x1, color=colour, alpha=0.15, lw=0)
        handles.append(Patch(color=colour, alpha=0.4, label=label))
    for item, colour in zip(args.event, EVENT_COLOURS):
        label, iso = item.split(":", 1)
        x = mdates.date2num(Time(iso).datetime)
        for ax in axs:
            ax.axvline(x, color=colour, lw=1.0, ls="--")
        handles.append(Line2D([], [], color=colour, ls="--", label=f"{label} {iso[11:19]}"))
    if handles:
        top.legend(handles=handles, loc="upper left", fontsize=8, ncol=len(handles), framealpha=0.85)
    axs[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    axs[-1].set_xlim(mdates.date2num(start.datetime), mdates.date2num(end.datetime))
    axs[-1].set_xlabel("UTC at Earth")
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=110)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
