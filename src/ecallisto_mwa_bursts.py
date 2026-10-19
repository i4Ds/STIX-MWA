#!/usr/bin/env python
"""Rank e-Callisto radio bursts that happened during archived MWA Sun observations.

Step "lists"     download the monthly e-Callisto burst lists (FHNW server), parse
                 them and match each burst with MWA Sun observations (output of
                 `mwa_stix_overlap.py mwa`).
Step "strength"  measure each burst in the Australia-ASSA spectrometers that
                 cover the MWA band, only while MWA was observing.
Step "rank"      rank bursts and list the MWA obs that contain the burst peak.
Step "vet"       plot ASSA spectrograms of the top bursts for a visual check.
Step "calculon"  list the MWA obsids already on calculon (one `ls` over ssh).
Step "candidates" join the ranking with MWA metadata, calibrators and the
                 calculon list; assign tiers A/B/C (no STIX at this stage).
Step "pictures"  one e-Callisto picture per tier A/B candidate, marking what
                 would be imaged, plus an HTML gallery for review.

ASSA receivers in the MWA band: ASSA_60 (108-524 MHz) until December 2023,
then ASSA_56 and ASSA_57 (108-377 MHz). ASSA_62/63 are 15-88 MHz and not used.

Strength: per time step, the 75th percentile across 110-300 MHz channels of
(data - per-channel median over the window +-10 min), smoothed over 3 samples;
the peak of that inside the MWA observing intervals. With two receivers the
ranking value is the smaller of the two, so both must see the burst; with one
(ASSA_60 era) it is that receiver's value and the burst is flagged single_rx.
Units are raw e-Callisto digits (relative, not calibrated flux), and ASSA_60
digits are not on the same scale as ASSA_56/57.

Why not MRO: MRO is Metsähovi, Finland, where the Sun is low or down during
MWA hours (about 00-08 UTC). MRO_60 also has a ~1 min full-band calibration
block at fixed times (e.g. hh:00 and hh:30) and MRO_59 is dominated by RFI;
both produced false peaks. Data within 5 s of the 15-min file boundaries is
masked.

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
INSTS = {"ASSA_56": "*Australia-ASSA_*_56.fit.gz", "ASSA_57": "*Australia-ASSA_*_57.fit.gz",
         "ASSA_60": "*Australia-ASSA_*_60.fit.gz"}
PAIR = ["ASSA_56", "ASSA_57"]
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
    for k in INSTS:
        if k not in s:
            s[k], s[k + "_t"] = np.nan, None
    both = s[PAIR].notna().all(axis=1)
    s["n_rx"] = s[list(INSTS)].notna().sum(axis=1)
    s["single_rx"] = ~both
    s["strength"] = np.where(both, s[PAIR].min(axis=1), s[list(INSTS)].max(axis=1))
    s["t_peak"] = pd.to_datetime(s.ASSA_57_t.fillna(s.ASSA_56_t).fillna(s.ASSA_60_t), format="mixed")
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


# --------------------------------------------------------------------- calculon
CALCULON_DIRS = ["/mnt/nas05/data02/MWA_data/data/mwa_data", "/mnt/nas05/data02/MWA_data/data/mwa_data2"]


def cmd_calculon(args):
    """One `ls` of the MWA data directories on calculon (login node: no du/find)."""
    import subprocess
    out = subprocess.run(["ssh", "-o", "BatchMode=yes", "calculon", "ls " + " ".join(CALCULON_DIRS)],
                         capture_output=True, text=True, check=True).stdout
    ids = sorted({int(m) for m in re.findall(r"^(\d{10})_", out, re.M)})
    with open(args.out, "w") as f:
        f.write(f"# MWA obsids in {' '.join(CALCULON_DIRS)}, listed {pd.Timestamp.now(tz='UTC'):%Y-%m-%d}\n")
        f.writelines(f"{i}\n" for i in ids)
    log(f"{len(ids)} obsids on calculon -> {args.out}")


def read_calculon(path):
    return {int(l) for l in open(path) if l.strip() and not l.startswith("#")} if os.path.exists(path) else set()


# ------------------------------------------------------------------- candidates
CAL_COLS = ["obs_id", "starttime_utc", "obsname", "channel_numbers_csv", "total_archived_data_bytes"]


def calibrators(days, cache):
    """Archived calibrator obs (mwa.observation.calibration) within a day of each candidate day."""
    import pyvo
    from astropy.time import Time
    have = pd.read_csv(cache) if os.path.exists(cache) else pd.DataFrame(columns=CAL_COLS + ["day"])
    todo = sorted(set(days) - set(have.day.astype(str)))
    if todo:
        tap = pyvo.dal.TAPService("https://vo.mwatelescope.org/mwa_asvo/tap")
        parts = [have]
        for d in todo:
            g0 = Time(pd.Timestamp(d) - pd.Timedelta(days=1)).gps
            g1 = Time(pd.Timestamp(d) + pd.Timedelta(days=2)).gps
            q = (f"SELECT {', '.join(CAL_COLS)} FROM mwa.observation WHERE obs_id >= {int(g0)} "
                 f"AND obs_id < {int(g1)} AND calibration = 'True' AND deleted_flag != 'TRUE'")
            t = tap.run_sync(q, maxrec=tap.hardlimit).to_table().to_pandas()
            t["day"] = d
            parts.append(t)
            log(f"calibrators {d}: {len(t)}")
        have = pd.concat(parts, ignore_index=True)
        have.to_csv(cache, index=False)
    have["t"] = pd.to_datetime(have.starttime_utc, format="mixed").dt.tz_localize(None)
    return have.drop_duplicates("obs_id")


def obs_access(ids, cache):
    """projectid and end of embargo per obsid (mwa.observation), cached in a CSV."""
    import pyvo
    have = pd.read_csv(cache) if os.path.exists(cache) else pd.DataFrame(
        columns=["obs_id", "projectid", "public_release_date_utc"])
    todo = sorted(set(int(i) for i in ids) - set(have.obs_id.astype(int)))
    if todo:
        tap = pyvo.dal.TAPService("https://vo.mwatelescope.org/mwa_asvo/tap")
        parts = [have]
        for k in range(0, len(todo), 200):
            q = ("SELECT obs_id, projectid, public_release_date_utc FROM mwa.observation WHERE obs_id IN ("
                 + ",".join(map(str, todo[k:k + 200])) + ")")
            parts.append(tap.run_sync(q, maxrec=tap.hardlimit).to_table().to_pandas())
        have = pd.concat(parts, ignore_index=True)
        have.to_csv(cache, index=False)
    return have.drop_duplicates("obs_id").set_index("obs_id")


def embargoed(projectid, release, now=None):
    """MWA Data Access Policy: data are restricted for 18 months, but guaranteed-time projects
    (G...) are open to collaboration members at once; open-access (OA...) data go to the PI's
    team only, engineering (C...) to MWA operations. So: blocked = still under embargo and not G."""
    now = now or pd.Timestamp.now(tz="UTC")
    rel = pd.to_datetime(release, utc=True, errors="coerce")
    return (rel > now) & ~pd.Series(projectid, index=rel.index).astype(str).str.startswith("G")


def cmd_candidates(args):
    s = pd.read_csv(args.ranking, parse_dates=["t_peak"])
    s = s[s.mwa_obs_at_peak.notna()].copy()
    s["rank"] = np.arange(1, len(s) + 1)
    s["mwa_obs_id"] = s.mwa_obs_at_peak.astype(str).str.split()
    s = s.explode("mwa_obs_id")
    s["mwa_obs_id"] = s.mwa_obs_id.astype(float).astype(int)
    m = pd.read_csv(args.mwa, parse_dates=["t_start", "t_stop"])
    keep = ["obs_id", "t_start", "t_stop", "duration_s", "projectid", "obsname", "dataqualityname", "good_tiles",
            "sun_elevation", "mwa_array_configuration", "channel_numbers_csv", "int_time", "freq_res",
            "total_archived_data_bytes"]
    c = s.merge(m[keep].add_prefix("mwa_"), on="mwa_obs_id", how="left")
    c["on_calculon"] = c.mwa_obs_id.isin(read_calculon(args.calculon))
    acc = obs_access(c.mwa_obs_id.unique(), args.access_cache)
    c["mwa_public_release"] = c.mwa_obs_id.map(acc.public_release_date_utc).astype(str).str[:10]
    c["embargoed"] = embargoed(c.mwa_obs_id.map(acc.projectid), c.mwa_obs_id.map(acc.public_release_date_utc)).values
    c["margin_s"] = np.minimum((c.t_peak - c.mwa_t_start).dt.total_seconds(),
                               (c.mwa_t_stop - c.t_peak).dt.total_seconds())

    cals = calibrators(sorted(c.t_peak.dt.strftime("%Y-%m-%d").unique()), args.cal_cache)
    cals = cals[cals.total_archived_data_bytes.fillna(0) > 0]
    on_calc = read_calculon(args.calculon)
    best = []
    for r in c.itertuples():
        q = cals[cals.channel_numbers_csv == r.mwa_channel_numbers_csv]
        if q.empty:
            best.append((None, None, np.nan, False))
            continue
        dt = (q.t - r.t_peak).abs()
        q = q.assign(dt_hr=dt.dt.total_seconds() / 3600, here=q.obs_id.isin(on_calc))
        q = q[q.dt_hr <= 24].sort_values(["here", "dt_hr"], ascending=[False, True])
        if q.empty:
            best.append((None, None, np.nan, False))
        else:
            b = q.iloc[0]
            best.append((int(b.obs_id), b.obsname, round(b.dt_hr, 1), bool(b.here)))
    c["cal_obs_id"], c["cal_name"], c["cal_dt_hr"], c["cal_on_calculon"] = zip(*best)

    typed = ~c.type.astype(str).str.strip().isin(["---", "", "nan"])
    mwa_ok = ((c.mwa_total_archived_data_bytes.fillna(0) > 0) & (c.margin_s >= 8) & (c.mwa_duration_s >= 56)
              & (c.mwa_dataqualityname == "Good") & (c.mwa_sun_elevation >= 30))
    cal_ok = c.cal_obs_id.notna()
    # strength thresholds in raw digits; ASSA_60 is on its own scale
    thr = np.where(c.single_rx, args.thr60, args.thr)
    strong, moderate = c.strength >= thr, c.strength >= thr / 2
    a = strong & typed & (c.n_stations >= 3) & mwa_ok & cal_ok
    b = ~a & moderate & (c.n_stations >= 2) & (c.margin_s >= 4) & (c.mwa_total_archived_data_bytes.fillna(0) > 0)
    c["tier"] = np.select([a, b], ["A", "B"], "C")
    c.loc[c.embargoed, "tier"] = "E"   # not downloadable for us before mwa_public_release
    c["why_not_A"] = [", ".join(k for k, ok in [("weak", st), ("untyped", ty), ("few stations", ns), ("MWA", mo),
                                               ("no calibrator", co)] if not ok)
                      for st, ty, ns, mo, co in zip(strong, typed, c.n_stations >= 3, mwa_ok, cal_ok)]
    # Phase IIIb first, then data already on calculon. The archive labels everything from
    # 2024-10-28 "Phase III"; all 256 tiles are correlated only from about April 2025 (2025B
    # in the MWA wiki). No IIIb label exists, so IIIb here = Phase III from 2025-04-01 with
    # > 128 good tiles (Dec 2024 has obs with 132-135 good tiles, before 256-tile correlation).
    p3 = c.mwa_mwa_array_configuration.astype(str).str.contains("III")
    c["phase"] = np.where(p3 & (c.mwa_good_tiles > 128) & (c.mwa_t_start >= "2025-04-01"), "IIIb",
                          c.mwa_mwa_array_configuration.astype(str).str.replace("Phase ", "", regex=False))
    c["phase3b"] = c.phase == "IIIb"
    c = c.sort_values(["tier", "phase3b", "on_calculon", "strength"], ascending=[True, False, False, False])
    # the burst lists can hold the same burst twice (e.g. typed and untyped); keep the best tier
    c = c.drop_duplicates(["mwa_obs_id", "t_peak"])
    cols = ["tier", "phase", "phase3b", "rank", "t_peak", "type", "strength", "single_rx", "n_rx", "n_stations", "mwa_obs_id",
            "on_calculon", "mwa_projectid", "mwa_t_start", "mwa_duration_s", "margin_s", "mwa_mwa_array_configuration",
            "mwa_channel_numbers_csv", "mwa_int_time", "mwa_freq_res", "mwa_good_tiles", "mwa_sun_elevation",
            "mwa_dataqualityname", "embargoed", "mwa_public_release", "cal_obs_id", "cal_name", "cal_dt_hr",
            "cal_on_calculon",
            "why_not_A", "start", "end", "stations"]
    c[cols].to_csv(args.out, index=False)
    log(f"{len(c)} candidates: " + ", ".join(f"{k} {v}" for k, v in c.tier.value_counts().sort_index().items())
        + f"; tier A on calculon {int((c.on_calculon & (c.tier == 'A')).sum())} -> {args.out}")


# --------------------------------------------------------------------- pictures
def channel_curves(d, chans):
    """Per MWA coarse channel: ASSA excess divided by that frequency's noise (MAD over the
    +-10 min window), as a time series averaged over the receivers. RFI stripes have a high
    excess all the time and so a high noise, which keeps them from winning."""
    out = {}
    for df in d.values():
        ex_ = df - df.median()
        snr = ex_ / (1.4826 * ex_.abs().median()).replace(0, np.nan)
        f = snr.columns.values.astype(float)
        for ch in chans:
            m = np.abs(f - ch * 1.28) <= 0.7
            if m.any():
                s = snr.loc[:, m].median(axis=1).rolling("4s").mean()
                out.setdefault(ch, []).append(s)
    return {ch: pd.concat(v, axis=1).mean(axis=1) for ch, v in out.items()}


def proposal(d, r, chans):
    """Channels and times to image. Series window: where the broadband excess stays above 25 % of
    the peak (+-10 s, inside the obs). Channels: highest mean S/N over that window (top 3).
    4 s image: where the best channel's 4 s mean S/N peaks inside the window."""
    df = next(iter(d.values()))
    bb = broadband(df)
    t0 = t1 = r.t_peak
    if bb is not None and not bb.empty:
        pk = bb[(bb.index >= r.t_peak - pd.Timedelta(seconds=5)) & (bb.index <= r.t_peak + pd.Timedelta(seconds=5))].max()
        above = bb >= 0.25 * pk
        i = bb.index.get_indexer([r.t_peak], method="nearest")[0]
        j0 = j1 = i
        while j0 > 0 and above.iloc[j0 - 1]:
            j0 -= 1
        while j1 < len(bb) - 1 and above.iloc[j1 + 1]:
            j1 += 1
        t0, t1 = bb.index[j0], bb.index[j1]
    t0 = max(t0 - pd.Timedelta(seconds=10), r.mwa_t_start)
    t1 = min(t1 + pd.Timedelta(seconds=10), r.mwa_t_stop)
    curves = channel_curves(d, chans)
    score = pd.Series({ch: s[(s.index >= t0) & (s.index <= t1)].mean() for ch, s in curves.items()}).dropna()
    score = score.sort_values(ascending=False)
    if score.empty:
        return None, [], r.t_peak, t0, t1
    ch = int(score.index[0])
    # keep the 4 s image >= 8 s inside the obs (the first seconds are flagged as quack time)
    a = max(t0, r.mwa_t_start + pd.Timedelta(seconds=8)) + pd.Timedelta(seconds=4)
    b = min(t1, r.mwa_t_stop - pd.Timedelta(seconds=8))
    s = curves[ch][(curves[ch].index >= a) & (curves[ch].index <= b)]
    t_img = s.idxmax() - pd.Timedelta(seconds=2) if not s.empty else r.t_peak   # rolling("4s") is right-labelled
    return ch, [int(x) for x in score.index[:3]], t_img, t0, t1


def cmd_pictures(args):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list(
        "b", ["#ffffff", "#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])
    c = pd.read_csv(args.candidates, parse_dates=["t_peak", "mwa_t_start"])
    keep = (c.tier.isin(args.tiers.split(",")) | (c.phase3b & (c.strength >= args.iiib_min))) & ~c.embargoed
    c = c[keep].head(args.n)
    c["mwa_t_stop"] = c.mwa_t_start + pd.to_timedelta(c.mwa_duration_s, unit="s")
    os.makedirs(args.outdir, exist_ok=True)

    def get(r):
        t0, t1 = r.t_peak - pd.Timedelta(minutes=10), r.t_peak + pd.Timedelta(minutes=10)
        rx = PAIR if not r.single_rx else ["ASSA_60"]
        return {k: load(k, t0, t1) for k in rx}
    with ThreadPoolExecutor(6) as ex:
        data = list(ex.map(get, list(c.itertuples())))
    props = []
    for r, d in zip(c.itertuples(), data):
        d = {k: v.loc[:, (v.columns >= 108) & (v.columns <= 380)].astype(float)
             for k, v in d.items() if v is not None and len(v) > 50}
        chans = [int(x) for x in str(r.mwa_channel_numbers_csv).split(",") if x.strip().isdigit()]
        if not d:
            log(f"{r.t_peak} no ASSA data")
            continue
        ch, top3, t_img, s0, s1 = proposal(d, r, chans)
        props.append(dict(tier=r.tier, mwa_obs_id=r.mwa_obs_id, t_peak=r.t_peak, channel=ch,
                          freq_mhz=None if ch is None else round(ch * 1.28, 2),
                          series_channels=" ".join(map(str, top3)), image_utc=t_img,
                          image_4s=f"{t_img - pd.Timedelta(seconds=2):%H:%M:%S}-{t_img + pd.Timedelta(seconds=2):%H:%M:%S}",
                          series_t0=s0, series_t1=s1, cal_obs_id=r.cal_obs_id, on_calculon=r.on_calculon))
        fig, axs = plt.subplots(len(d), 2, figsize=(13, 2.9 * len(d) + 0.6), squeeze=False,
                                constrained_layout=True, gridspec_kw=dict(width_ratios=[2, 1]))
        fmin, fmax = (min(chans) - 0.5) * 1.28, (max(chans) + 0.5) * 1.28
        for row, (k, df) in zip(axs, d.items()):
            ex_ = df - df.median()
            for ax, half in zip(row, [pd.Timedelta(minutes=10), pd.Timedelta(seconds=60)]):
                w = ex_[(ex_.index >= r.t_peak - half) & (ex_.index <= r.t_peak + half)]
                ax.pcolormesh(w.index, w.columns, w.T.values, cmap=cmap, vmin=0,
                              vmax=max(5, np.nanpercentile(w.values, 99.7)), shading="nearest", rasterized=True)
                ax.axvspan(max(r.mwa_t_start, w.index[0]), min(r.mwa_t_stop, w.index[-1]),
                           facecolor="#eb6834", alpha=0.12, edgecolor="#eb6834", lw=1.5)
                for fl in (fmin, fmax):
                    ax.axhline(fl, color="#eb6834", lw=0.8, ls=":")
                for k_, c_ in enumerate(top3):
                    ax.axhline(c_ * 1.28, color="#d6262b", lw=1.2 if k_ == 0 else 0.7, ls="--" if k_ == 0 else ":")
                ax.axvspan(max(s0, w.index[0]), min(s1, w.index[-1]), ymin=0, ymax=0.04, color="#d6262b")
                ax.axvline(t_img, color="#d6262b", lw=0.8)
                ax.set_xlim(w.index[0], w.index[-1])
                ax.set_ylim(380, 108)
                ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=7 if half > pd.Timedelta(minutes=2) else 4))
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M" if half > pd.Timedelta(minutes=2) else "%H:%M:%S"))
                ax.tick_params(labelsize=8)
            row[0].set_ylabel(f"{k}\nMHz", fontsize=8)
        cal ="no calibrator" if pd.isna(r.cal_obs_id) else (
            f"cal {int(r.cal_obs_id)} ({str(r.cal_name).split('_Ch')[0]}, {r.cal_dt_hr} h"
            f"{', on calculon' if r.cal_on_calculon else ''})")
        fig.suptitle(
            f"Tier {r.tier}  {r.t_peak:%Y-%m-%d %H:%M:%S}  type {r.type}  strength {r.strength:g}"
            f"{' (single rx)' if r.single_rx else ''}  {r.n_stations} stations\n"
            f"MWA {r.mwa_obs_id} {r.mwa_projectid} {r.mwa_t_start:%H:%M:%S}+{r.mwa_duration_s:.0f}s "
            f"Phase {r.phase} ({r.mwa_good_tiles:.0f} good tiles)  {'ON CALCULON' if r.on_calculon else 'not on calculon'}  {cal}\n"
            f"Proposed: ch {ch} ({'' if ch is None else f'{ch * 1.28:.1f} MHz'}, red dashed), series channels "
            f"{' '.join(map(str, top3))} (dotted); 4 s image at {t_img:%H:%M:%S} (red line); "
            f"series {s0:%H:%M:%S}-{s1:%H:%M:%S} (red bar). Orange: MWA obs and band.",
            x=0.01, ha="left", fontsize=9)
        fn = os.path.join(args.outdir, f"{r.tier}_{r.t_peak:%Y%m%d_%H%M%S}_{r.mwa_obs_id}.png")
        fig.savefig(fn, dpi=80)
        plt.close(fig)
        log(fn)
    pd.DataFrame(props).to_csv(os.path.join(args.outdir, "proposals.csv"), index=False)


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
    a = sub.add_parser("calculon")
    a.add_argument("--out", default="calculon_obsids.txt")
    a.set_defaults(func=cmd_calculon)
    a = sub.add_parser("candidates")
    a.add_argument("--ranking", default="burst_ranking.csv")
    a.add_argument("--mwa", default="mwa_sun_obs.csv")
    a.add_argument("--calculon", default="calculon_obsids.txt")
    a.add_argument("--cal-cache", default="calibrators.csv")
    a.add_argument("--access-cache", default="obs_access.csv", help="projectid and embargo end per obsid")
    a.add_argument("--thr", type=float, default=5.0, help="tier A strength, ASSA_56/57 digits")
    a.add_argument("--thr60", type=float, default=5.0, help="tier A strength, ASSA_60 digits")
    a.add_argument("--out", default="candidates.csv")
    a.set_defaults(func=cmd_candidates)
    a = sub.add_parser("pictures")
    a.add_argument("--candidates", default="candidates.csv")
    a.add_argument("--tiers", default="A,B")
    a.add_argument("--n", type=int, default=80)
    a.add_argument("--iiib-min", type=float, default=3.0, help="also show Phase IIIb rows of any tier at this strength")
    a.add_argument("--outdir", default="pictures")
    a.set_defaults(func=cmd_pictures)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
