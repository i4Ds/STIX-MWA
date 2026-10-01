#!/usr/bin/env python
"""Find MWA observations of the Sun that overlap in time with STIX flares.

Step 1  MWA: query the MWA ASVO TAP service (same endpoint and pyvo approach as
        MWATelescope/mwa-demo demo/01_tap.py) for daytime observations
        (sun_elevation > 0), then keep those whose pointing centre lies within
        --max-sep degrees of the Sun (Sun position computed with astropy).
Step 2  STIX: read the STIX science flare list (Hayes et al.,
        github.com/hayesla/stix_flarelist_science) and convert the Solar Orbiter
        times to Earth-arrival times.
Step 3  Match flare intervals (padded by --pad seconds) against MWA observation
        intervals and write the pairs to CSV.

Usage
  python mwa_stix_overlap.py mwa  [--start 2021-01-01] [--out mwa_sun_obs.csv]
  python mwa_stix_overlap.py match --stix STIX_flarelist_...csv \
         [--mwa mwa_sun_obs.csv] [--out mwa_stix_overlap.csv]

Needs network access to vo.mwatelescope.org for the "mwa" step.
Dependencies: pyvo, astropy, pandas, numpy.
"""

import argparse
import sys

import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import SkyCoord, get_sun
from astropy.time import Time

TAP_URL = "http://vo.mwatelescope.org/mwa_asvo/tap"  # as in mwa-demo 01_tap.py
LIGHT_S_PER_AU = (1 * u.au / (299792.458 * u.km / u.s)).to(u.s).value  # ~499.005 s

# Columns we would like from mwa.observation. Only those that actually exist on
# the server are requested (checked with a TOP 1 query), so nothing is assumed.
WANTED = [
    "obs_id", "starttime_utc", "stoptime_utc", "obsname", "projectid",
    "ra_pointing", "dec_pointing", "sun_elevation", "sun_pointing_distance",
    "mwa_array_configuration", "channel_numbers_csv", "freq_res", "int_time",
    "good_tiles", "dataquality", "gpubox_files_archived",
    "total_archived_data_bytes", "deleted_flag", "mode",
]
STOP_CANDIDATES = ["stoptime_utc", "stoptime_gps", "stoptime_mjd"]


def log(*a):
    print(*a, file=sys.stderr)


# --------------------------------------------------------------------------- MWA
def mwa_columns(tap):
    t = tap.search("SELECT TOP 1 * FROM mwa.observation").to_table()
    return list(t.colnames)


def query_chunk(tap, cols, gps0, gps1):
    q = f"""
SELECT {', '.join(cols)}
FROM mwa.observation
WHERE obs_id >= {int(gps0)} AND obs_id < {int(gps1)}
AND sun_elevation > 0
AND deleted_flag != 'TRUE'
"""
    return tap.run_sync(q, maxrec=tap.hardlimit).to_table().to_pandas()


def query_mwa(start, stop, days_per_chunk=30):
    import pyvo

    tap = pyvo.dal.TAPService(TAP_URL)
    have = mwa_columns(tap)
    log(f"mwa.observation has {len(have)} columns")
    cols = [c for c in WANTED if c in have]
    missing = [c for c in WANTED if c not in have]
    if missing:
        log("not on server (skipped):", ", ".join(missing))
    if not any(c in have for c in STOP_CANDIDATES):
        sys.exit(f"no stop-time column found among {STOP_CANDIDATES}; "
                 f"available: {have}")

    t0, t1 = Time(start), Time(stop)
    edges = np.arange(t0.gps, t1.gps + days_per_chunk * 86400, days_per_chunk * 86400)
    parts = []
    for a, b in zip(edges[:-1], edges[1:]):
        df = query_chunk(tap, cols, a, b)
        # if a hard row limit truncated the chunk, split it
        lim = tap.hardlimit
        if lim and len(df) >= lim:
            mid = (a + b) / 2
            df = pd.concat([query_chunk(tap, cols, a, mid), query_chunk(tap, cols, mid, b)])
        log(f"{Time(a, format='gps').iso[:10]}  {len(df):6d} daytime obs")
        parts.append(df)
    return pd.concat(parts, ignore_index=True)


def add_times_and_sun(df):
    start = Time(df["obs_id"].astype(float).values, format="gps")
    if "stoptime_utc" in df:
        stop = Time(pd.to_datetime(df["stoptime_utc"]).dt.tz_localize(None).values)
    elif "stoptime_gps" in df:
        stop = Time(df["stoptime_gps"].astype(float).values, format="gps")
    else:
        stop = Time(df["stoptime_mjd"].astype(float).values, format="mjd")
    df["t_start"] = start.utc.datetime64
    df["t_stop"] = stop.utc.datetime64
    df["duration_s"] = (stop - start).to_value(u.s)

    mid = start + (stop - start) / 2
    sun = get_sun(mid)
    point = SkyCoord(df["ra_pointing"].values * u.deg, df["dec_pointing"].values * u.deg,
                     frame="icrs")
    df["sun_sep_deg"] = point.separation(sun.transform_to("icrs")).deg
    if "obsname" in df:
        df["name_says_sun"] = df["obsname"].astype(str).str.contains("sun|solar", case=False)
    return df


def cmd_mwa(args):
    df = query_mwa(args.start, args.stop)
    df = df.dropna(subset=["ra_pointing", "dec_pointing"])
    df = add_times_and_sun(df)
    sel = df["sun_sep_deg"] <= args.max_sep
    if "name_says_sun" in df:
        sel |= df["name_says_sun"]
    out = df[sel].sort_values("obs_id")
    out.to_csv(args.out, index=False)
    log(f"{len(df)} daytime obs, {len(out)} Sun obs (sep <= {args.max_sep} deg "
        f"or name contains sun/solar) -> {args.out}")


# -------------------------------------------------------------------------- STIX
def load_stix(path):
    s = pd.read_csv(path)
    t_start = Time(list(s["start_UTC"]))
    # Earth-Sun distance from astropy (GCRS distance of the Sun)
    d_earth_au = get_sun(t_start).distance.to_value(u.au)
    # photons from the Sun reach Earth later than Solar Orbiter by
    # (d_Earth - d_SolO) / c  (radial approximation, flare near disk centre)
    s["earth_shift_s"] = (d_earth_au - s["solo_position_AU_distance"]) * LIGHT_S_PER_AU
    for c in ["start_UTC", "peak_UTC", "end_UTC"]:
        s[c.replace("_UTC", "_earth")] = (pd.to_datetime(s[c])
                                          + pd.to_timedelta(s["earth_shift_s"], unit="s"))
    return s


# ------------------------------------------------------------------------- match
def match(stix, mwa, pad_s):
    fs = (stix["start_earth"] - pd.Timedelta(seconds=pad_s)).values
    fe = (stix["end_earth"] + pd.Timedelta(seconds=pad_s)).values
    ms = pd.to_datetime(mwa["t_start"]).values
    me = pd.to_datetime(mwa["t_stop"]).values
    order = np.argsort(ms)
    ms, me = ms[order], me[order]
    max_dur = (me - ms).max()
    rows = []
    for i in range(len(stix)):
        # candidate obs: start < flare end and start > flare start - longest obs
        lo = np.searchsorted(ms, fs[i] - max_dur, side="left")
        hi = np.searchsorted(ms, fe[i], side="left")
        for j in range(lo, hi):
            ov = min(me[j], fe[i]) - max(ms[j], fs[i])
            if ov > np.timedelta64(0, "s"):
                rows.append((i, order[j], ov / np.timedelta64(1, "s")))
    if not rows:
        return pd.DataFrame()
    fi, mj, ov = map(np.array, zip(*rows))
    keep_s = ["flare_id", "start_UTC", "peak_UTC", "end_UTC", "start_earth", "peak_earth",
              "end_earth", "earth_shift_s", "4-10 keV", "25-50 keV", "att_in",
              "visible_from_earth", "hpc_x_earth", "hpc_y_earth",
              "GOES_class_time_of_flare", "goes_estimated_mean_class"]
    keep_s = [c for c in keep_s if c in stix]
    a = stix.iloc[fi][keep_s].reset_index(drop=True)
    b = mwa.iloc[mj].reset_index(drop=True).add_prefix("mwa_")
    out = pd.concat([a, b], axis=1)
    out["overlap_s"] = ov
    return out


def cmd_match(args):
    stix = load_stix(args.stix)
    mwa = pd.read_csv(args.mwa)
    log(f"{len(stix)} STIX flares, {len(mwa)} MWA Sun obs")
    out = match(stix, mwa, args.pad)
    if out.empty:
        log("no overlaps")
    else:
        out = out.sort_values(["start_earth", "mwa_obs_id"])
        log(f"{len(out)} flare/obs pairs, {out['flare_id'].nunique()} flares, "
            f"{out['mwa_obs_id'].nunique()} MWA obs")
    out.to_csv(args.out, index=False)
    log(f"-> {args.out}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("mwa", help="query MWA TAP for Sun observations")
    m.add_argument("--start", default="2021-01-01", help="UTC (STIX list starts 2021-02)")
    m.add_argument("--stop", default=Time.now().iso[:10])
    m.add_argument("--max-sep", type=float, default=15.0,
                   help="max pointing-centre to Sun separation, deg")
    m.add_argument("--out", default="mwa_sun_obs.csv")
    m.set_defaults(func=cmd_mwa)
    x = sub.add_parser("match", help="cross-match STIX flares with MWA Sun obs")
    x.add_argument("--stix", required=True)
    x.add_argument("--mwa", default="mwa_sun_obs.csv")
    x.add_argument("--pad", type=float, default=60.0, help="pad flare interval, s")
    x.add_argument("--out", default="mwa_stix_overlap.csv")
    x.set_defaults(func=cmd_match)
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
