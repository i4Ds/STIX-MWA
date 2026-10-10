"""
Stack e-Callisto 15-min FITS files per station/focus code and plot dynamic spectra.

Files are named STATION_YYYYMMDD_HHMMSS_FOCUS.fit.gz as on
http://soleil80.cs.technik.fhnw.ch/solarradio/data/2002-20yy_Callisto/.

    python src/ecallisto_spectrogram.py files/ecallisto/20220930 \
        --start 2022-09-30T03:30 --end 2022-09-30T04:30 --out _results/plots/ecallisto_20220930.png
"""
import argparse
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from astropy.time import Time

NAME = re.compile(r"(?P<station>.+)_(?P<date>\d{8})_(?P<time>\d{6})_(?P<focus>\d{2})\.fit(\.gz)?$")


@dataclass
class Spectrogram:
    station: str
    data: np.ndarray  # (nfreq, ntime), increasing frequency
    freq: np.ndarray  # MHz
    time: np.ndarray  # datetime64[ms]

    def background_subtracted(self) -> np.ndarray:
        """Subtract each channel's median over time (constant background)."""
        return self.data - np.nanmedian(self.data, axis=1, keepdims=True)


def read(path: Path) -> Spectrogram:
    with fits.open(path) as hdul:
        data = np.asarray(hdul[0].data, dtype=float)
        hd = hdul[0].header
        t_rel = np.asarray(hdul[1].data["TIME"][0], dtype=float)
        freq = np.asarray(hdul[1].data["FREQUENCY"][0], dtype=float)
    t0 = Time(hd["DATE-OBS"].replace("/", "-") + "T" + hd["TIME-OBS"])
    # The TIME column is seconds from TIME-OBS on some stations, of day (CRVAL1) on others.
    if t_rel[0] > 0.5 * hd.get("CRVAL1", np.inf):
        t_rel = t_rel - hd["CRVAL1"]
    time = (t0.datetime64 + (t_rel * 1e3).astype("timedelta64[ms]")).astype("datetime64[ms]")
    order = np.argsort(freq)
    return Spectrogram(hd.get("INSTRUME", path.name), data[order], freq[order], time)


def stack(paths: list[Path], start: Time, end: Time) -> Spectrogram | None:
    parts = [read(p) for p in sorted(paths)]
    parts = [s for s in parts if s.data.shape[0] == parts[0].data.shape[0]
             and np.allclose(s.freq, parts[0].freq, atol=0.01)]
    if not parts:
        return None
    data = np.concatenate([s.data for s in parts], axis=1)
    time = np.concatenate([s.time for s in parts])
    keep = (time >= start.datetime64) & (time <= end.datetime64)
    if not keep.any():
        return None
    return Spectrogram(parts[0].station, data[:, keep], parts[0].freq, time[keep])


def by_station(directory: Path) -> dict[str, list[Path]]:
    groups = defaultdict(list)
    for p in directory.iterdir():
        m = NAME.match(p.name)
        if m:
            groups[f"{m['station']}_{m['focus']}"].append(p)
    return dict(sorted(groups.items()))


def show(ax, spec: Spectrogram, pct=(5, 99.5), cmap="inferno"):
    img = spec.background_subtracted()
    vmin, vmax = np.nanpercentile(img, pct)
    t = mdates.date2num(spec.time.astype("datetime64[ms]").astype(object))
    ax.pcolormesh(t, spec.freq, img, shading="nearest", cmap=cmap, vmin=vmin, vmax=vmax, rasterized=True)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.set_ylabel("MHz")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("directory", type=Path)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    start, end = Time(args.start), Time(args.end)

    specs = {k: stack(v, start, end) for k, v in by_station(args.directory).items()}
    specs = {k: s for k, s in specs.items() if s is not None}
    fig, axs = plt.subplots(len(specs), 1, figsize=(14, 2.1 * len(specs)), sharex=True, squeeze=False)
    for ax, (key, spec) in zip(axs[:, 0], specs.items()):
        show(ax, spec)
        ax.set_title(f"{key}  {spec.freq.min():.0f}–{spec.freq.max():.0f} MHz", fontsize=9, loc="left")
        print(f"{key:28s} {spec.freq.min():6.1f}–{spec.freq.max():6.1f} MHz  {spec.data.shape}")
    axs[-1, 0].set_xlabel("UTC (Earth)")
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=90)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
