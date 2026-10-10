#!/usr/bin/env python
"""Rank e-Callisto radio bursts that happened during archived MWA Sun observations.

Step "lists"     download the monthly e-Callisto burst lists (FHNW server), parse
                 them and match each burst with MWA Sun observations (output of
                 `mwa_stix_overlap.py mwa`).
Step "strength"  measure each burst in the Australia-ASSA spectrometers
                 (ASSA_56, ASSA_57; 108-377 MHz), only while MWA was observing.
Step "rank"      rank bursts and list the MWA obs that contain the burst peak.
Step "vet"       plot ASSA spectrograms of the top bursts for a visual check.

Strength: per time step, the 75th percentile across 110-300 MHz channels of
(data - per-channel median over the window +-10 min), smoothed over 3 samples;
the peak of that inside the MWA observing intervals. The ranking value is the
smaller of ASSA_56 and ASSA_57, so both receivers must see the burst. Units are
raw e-Callisto digits (relative, not calibrated flux).

Why ASSA and not MRO: MRO_60 has a ~1 min full-band calibration block at fixed
times (e.g. hh:00 and hh:30) and MRO_59 is dominated by RFI; both produced false
peaks. Data within 5 s of the 15-min file boundaries is masked.

Usage
  python ecallisto_mwa_bursts.py lists    --mwa mwa_sun_obs.csv [--out burst_mwa_pairs.csv]
  python ecallisto_mwa_bursts.py strength [--pairs burst_mwa_pairs.csv] [--out burst_strength.jsonl]
  python ecallisto_mwa_bursts.py rank     [--pairs ...] [--strength ...] [--out burst_ranking.csv]
  python ecallisto_mwa_bursts.py vet      [--ranking burst_ranking.csv] [--n 12] [--out vet_top.png]

Dependencies: ecallisto_ng (i4Ds/ecallisto_ng), pandas, numpy, astropy, matplotlib, pyarrow.
"""

import argparse
import fnmatch
import glob
import json
import os
import re
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

BURST_LIST_URL = "http://soleil80.cs.technik.fhnw.ch/solarradio/data/BurstLists/2010-yyyy_Monstein"
INSTS = {"ASSA_56": "*Australia-ASSA_*_56.fit.gz", "ASSA_57": "*Australia-ASSA_*_57.fit.gz"}
F0, F1, EDGE_S = 110, 300, 5
MAX_BURST = pd.Timedelta(minutes=60)


def log(*a):
    print(*a, file=sys.stderr, flush=True)


# ------------------------------------------------------------------------ lists
def read_burst_lists(cache_dir, start_year=2021, stop_year=None):
    stop_year = stop_year or pd.Timestamp.now().year
    os.makedirs(cache_dir, exist_ok=True)
    rows = []
    for y in range(start_year, stop_year + 1):
        for m in range(1, 13):
            f = os.path.join(cache_dir, f"e-CALLISTO_{y}_{m:02d}.txt")
            if not os.path.exists(f):
                r = requests.get(f"{BURST_LIST_URL}/{y}/{os.path.basename(f)}", timeout=60)
                if r.status_code != 200:
                    continue
                open(f, "wb").write(r.content)
            for line in open(f, encoding="latin-1"):
                m_ = re.match(r"^(\d{8})\s+(\d\d:\d\d)-(\d\d:\d\d)\s+(\S+)\s*(.*)$", line.strip())
                if m_:
                    rows.append(m_.groups())
    b = pd.DataFrame(rows, columns=["date", "t0", "t1", "type", "stations"])
    b["start"] = pd.to_datetime(b.date + b.t0, format="%Y%m%d%H:%M", errors="coerce")
    b["end"] = pd.to_datetime(b.date + b.t1, format="%Y%m%d%H:%M", errors="coerce") + pd.Timedelta(seconds=59)
    b = b.dropna(subset=["start", "end"])           # drops "##:##" missing-data rows
    b = b[(b.start.dt.year >= start_year) & (b.start.dt.year <= stop_year)]   # drops typos (e.g. 2620)
    b.loc[b.end < b.start, "end"] += pd.Timedelta(days=1)
    b["n_stations"] = b.stations.str.count(",") + 1
    return b.reset_index(drop=True)


def cmd_lists(args):
    b = read_burst_lists(args.cache)
    log(f"{len(b)} bursts {b.start.min()} to {b.start.max()}")
    mwa = pd.read_csv(args.mwa).dropna(subset=["t_start", "t_stop"])
    mwa["t_start"], mwa["t_stop"] = pd.to_datetime(mwa.t_start), pd.to_datetime(mwa.t_stop)
    mwa = mwa.sort_values("t_start").reset_index(drop=True)
    ms, me = mwa.t_start.values, mwa.t_stop.values
    maxd = (me - ms).max()
    hits = []
    for i, r in b.iterrows():
        s0, e0 = np.datetime64(r.start), np.datetime64(r.end)
        for j in range(np.searchsorted(ms, s0 - maxd), np.searchsorted(ms, e0)):
            ov = (min(me[j], e0) - max(ms[j], s0)) / np.timedelta64(1, "s")
            if ov > 0:
                hits.append((i, j, ov))
    h = pd.DataFrame(hits, columns=["b", "m", "ov"])
    keep = ["obs_id", "t_start", "t_stop", "duration", "projectid", "obsname", "total_archived_data_bytes",
            "sun_sep_deg", "first_channel_lowest_frequency_mhz", "last_channel_highest_frequency_mhz"]
    out = pd.concat([b.loc[h.b].reset_index().rename(columns={"index": "burst_idx"}),
                     mwa.loc[h.m, keep].reset_index(drop=True).add_prefix("mwa_")], axis=1)
    out["overlap_s"] = h.ov.values
    out["mwa_archived"] = out.mwa_total_archived_data_bytes.fillna(0) > 0
    out.to_csv(args.out, index=False)
    log(f"{out.burst_idx.nunique()} bursts overlap MWA Sun obs, "
        f"{out[out.mwa_archived].burst_idx.nunique()} with archived data -> {args.out}")


# --------------------------------------------------------------------- strength
@lru_cache(maxsize=None)
def day_files(d):
    from ecallisto_ng.data_download.downloader import fetch_date_files, BASE_URL
    return tuple(fetch_date_files(f"{BASE_URL}/{d.year}/{d.month:02d}/{d.day:02d}/"))


def load(inst, t0, t1):
    """e-Callisto spectrogram (time x MHz) of one instrument between t0 and t1."""
    from astropy.io import fits
    from ecallisto_ng.data_download.utils import ecallisto_fits_to_pandas, extract_datetime_from_filename
    urls = []
    for d in pd.date_range(t0.normalize(), t1.normalize()):
        for u in fnmatch.filter(day_files(d.date()), INSTS[inst]):
            ft = extract_datetime_from_filename(u.split("/")[-1])
            if ft and t0 - pd.Timedelta(minutes=15) <= ft <= t1:
                urls.append(u)
    dfs = []
    for u in sorted(urls):
        try:
            with fits.open(u, cache=False) as f:
                dfs.append(ecallisto_fits_to_pandas(f))
        except Exception:
            pass
    if not dfs:
        return None
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated()]
    df = df.loc[:, ~df.columns.duplicated()].sort_index(axis=1)
    return df[(df.index >= t0) & (df.index <= t1)]


def broadband(df):
    df = df.loc[:, (df.columns >= F0) & (df.columns <= F1)].astype(float)
    if df.shape[1] < 40:
        return None
    bb = (df - df.median()).quantile(0.75, axis=1).rolling(3, center=True, min_periods=1).median()
    sec = (bb.index.minute % 15) * 60 + bb.index.second + bb.index.microsecond / 1e6
    return bb[(sec > EDGE_S) & (sec < 900 - EDGE_S)]


def measure(item):
    bi, g = item
    try:
        r0 = g.iloc[0]
        wins = [(max(r0.start, x.mwa_t_start), min(r0.end, x.mwa_t_stop)) for x in g.itertuples()]
        t0 = min(w[0] for w in wins) - pd.Timedelta(minutes=10)
        t1 = max(w[1] for w in wins) + pd.Timedelta(minutes=10)
        res = {"burst_idx": int(bi)}
        for inst in INSTS:
            df = load(inst, t0, t1)
            if df is None or len(df) < 50:
                continue
            bb = broadband(df)
            if bb is None or bb.empty:
                continue
            m = np.zeros(len(bb), bool)
            for a, b in wins:
                m |= (bb.index >= a) & (bb.index <= b)
            if m.any():
                w = bb[m]
                res[inst] = dict(peak=float(w.max()), t_peak=str(w.idxmax()),
                                 noise=float(1.4826 * (bb - bb.median()).abs().median()))
        return res
    except Exception as e:
        return {"burst_idx": int(bi), "error": repr(e)[:200]}


def read_pairs(path):
    p = pd.read_csv(path, parse_dates=["start", "end", "mwa_t_start", "mwa_t_stop"])
    return p[p.mwa_archived & ((p.end - p.start) <= MAX_BURST)]


def cmd_strength(args):
    p = read_pairs(args.pairs)
    done = {json.loads(l)["burst_idx"] for l in open(args.out)} if os.path.exists(args.out) else set()
    items = [(bi, g) for bi, g in p.groupby("burst_idx") if bi not in done]
    if args.limit:
        items = items[: args.limit]
    log(f"{len(items)} bursts to measure")
    with ThreadPoolExecutor(args.threads) as ex, open(args.out, "a") as f:
        for i, res in enumerate(ex.map(measure, items)):
            f.write(json.dumps(res) + "\n")
            f.flush()
            if i % 50 == 0:
                log(i)


# ------------------------------------------------------------------------- rank
def cmd_rank(args):
    rows = []
    for line in open(args.strength):
        r = json.loads(line)
        bi = r.pop("burst_idx")
        v = {k: r[k] for k in INSTS if k in r}
        if "error" in r or not v:
            continue
        d = {"burst_idx": bi}
        for k, x in v.items():
            d[k], d[k + "_t"] = x["peak"], x["t_peak"]
        rows.append(d)
    s = pd.DataFrame(rows)
    s["strength"] = s[list(INSTS)].min(axis=1)
    s["t_peak"] = pd.to_datetime(s.ASSA_57_t.fillna(s.ASSA_56_t), format="mixed")
    p = read_pairs(args.pairs)
    s = s.merge(p.groupby("burst_idx").agg(start=("start", "first"), end=("end", "first"),
                                           type=("type", "first"), n_stations=("n_stations", "first"),
                                           stations=("stations", "first")),
                left_on="burst_idx", right_index=True)

    def at_peak(bi, t):
        q = p[(p.burst_idx == bi) & (p.mwa_t_start <= t) & (p.mwa_t_stop >= t)]
        return (" ".join(map(str, q.mwa_obs_id)), " ".join(q.mwa_projectid),
                " ".join(q.mwa_duration.astype(int).astype(str)))
    c = [at_peak(b, t) for b, t in zip(s.burst_idx, s.t_peak)]
    s["mwa_obs_at_peak"], s["mwa_proj"], s["mwa_dur_s"] = zip(*c)
    s = s.sort_values("strength", ascending=False)
    s.to_csv(args.out, index=False)
    log(f"{len(s)} bursts ranked -> {args.out}")


# -------------------------------------------------------------------------- vet
def cmd_vet(args):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list(
        "b", ["#ffffff", "#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])
    s = pd.read_csv(args.ranking, parse_dates=["t_peak"]).drop_duplicates("t_peak").head(args.n)
    p = pd.read_csv(args.pairs, parse_dates=["mwa_t_start", "mwa_t_stop"])

    def get(r):
        t0, t1 = r.t_peak - pd.Timedelta(minutes=4), r.t_peak + pd.Timedelta(minutes=4)
        return load("ASSA_57", t0 - pd.Timedelta(minutes=6), t1 + pd.Timedelta(minutes=6)), t0, t1
    with ThreadPoolExecutor(6) as ex:
        data = list(ex.map(get, list(s.itertuples())))
    nrow = (len(s) + 1) // 2
    fig, axs = plt.subplots(nrow, 2, figsize=(13, 2.85 * nrow), constrained_layout=True, squeeze=False)
    for ax, r, (df, t0, t1) in zip(axs.flat, s.itertuples(), data):
        if df is None:
            ax.set_title(f"{r.t_peak} no ASSA data", loc="left", fontsize=8.5)
            continue
        df = df.loc[:, (df.columns >= 108) & (df.columns <= 380)].astype(float)
        ex_ = df - df.median()
        ex_ = ex_[(ex_.index >= t0) & (ex_.index <= t1)]
        ax.pcolormesh(ex_.index, ex_.columns, ex_.T.values, cmap=cmap, vmin=0,
                      vmax=max(5, np.nanpercentile(ex_.values, 99.7)), shading="nearest", rasterized=True)
        ax.invert_yaxis()
        for o in str(r.mwa_obs_at_peak).split():
            q = p[p.mwa_obs_id == int(o)].iloc[0]
            ax.axvspan(max(q.mwa_t_start, t0), min(q.mwa_t_stop, t1), facecolor="none", edgecolor="#eb6834", lw=2)
        ax.set_title(f"{r.t_peak:%Y-%m-%d %H:%M:%S}  {r.type}  str {r.strength:.0f}  "
                     f"MWA {r.mwa_obs_at_peak} ({r.mwa_proj}, {r.mwa_dur_s}s)", loc="left", fontsize=8.5)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.set_ylabel("MHz", fontsize=8)
    fig.suptitle("Australia-ASSA_57, background subtracted, ±4 min around peak; orange = MWA observation",
                 x=0.01, ha="left")
    fig.savefig(args.out, dpi=80)
    log(f"-> {args.out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("lists")
    a.add_argument("--mwa", required=True, help="MWA Sun obs CSV from mwa_stix_overlap.py mwa")
    a.add_argument("--cache", default="burst_lists")
    a.add_argument("--out", default="burst_mwa_pairs.csv")
    a.set_defaults(func=cmd_lists)
    a = sub.add_parser("strength")
    a.add_argument("--pairs", default="burst_mwa_pairs.csv")
    a.add_argument("--out", default="burst_strength.jsonl")
    a.add_argument("--threads", type=int, default=8)
    a.add_argument("--limit", type=int, default=0)
    a.set_defaults(func=cmd_strength)
    a = sub.add_parser("rank")
    a.add_argument("--pairs", default="burst_mwa_pairs.csv")
    a.add_argument("--strength", default="burst_strength.jsonl")
    a.add_argument("--out", default="burst_ranking.csv")
    a.set_defaults(func=cmd_rank)
    a = sub.add_parser("vet")
    a.add_argument("--ranking", default="burst_ranking.csv")
    a.add_argument("--pairs", default="burst_mwa_pairs.csv")
    a.add_argument("--n", type=int, default=12)
    a.add_argument("--out", default="vet_top.png")
    a.set_defaults(func=cmd_vet)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
