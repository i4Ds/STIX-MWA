from pathlib import Path
import shutil, logging, datetime, os
import numpy as np
from astropy.time import Time
from helper_functions.utils import get_observation_path, get_ms_files
import helper_functions.mwa_imaging as imaging
import helper_functions.calibration as cal

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


# -------------------- parameters to edit --------------------
runs = [
    # imaging only
    {
        "flare_id":        "1126847624",
        "observation_ids": ["1126847624"],
        "calibration_id":  None,
        "calibrator_flux_jy": 0.0,
    },
    # full calibration + imaging
    {
        "flare_id":        "1126847624",
        "observation_ids": ["1126847624"],
        "calibration_id":  "1126854528",
        "calibrator_flux_jy": 1670.0,
    },
]

 # roots
work_base = Path.cwd() / "tmp"
out_base  = Path.cwd().parent / "results" / "mwa_vids"


def run_job(cfg: dict, tag: str):
    """orchestrate one job – optional calibration plus imaging stack"""
    flare_id        = cfg["flare_id"]
    observation_ids = cfg["observation_ids"]
    calibration_id  = cfg["calibration_id"]
    flux_jy         = cfg["calibrator_flux_jy"]

    work_root = work_base / tag
    work_root.mkdir(parents=True, exist_ok=True)
    tmp_dirs: list[str] = []

     # optional calibration
    if calibration_id:
        sol_path = work_root / f"{tag}_cal_sols.fits"
        cal_ms, cal_tmp = fetch_ms(calibration_id)
        tmp_dirs.append(cal_tmp)
        cal.run_di_calibrate(cal_ms, flux_jy, sol_path)
    else:
        sol_path = None

     # imaging for each science ms
    cubes, axes = [], []
    try:
        for obs_id in observation_ids:
            raw_ms, tmp = fetch_ms(obs_id)
            tmp_dirs.append(tmp)

            ms_in = (
                cal.apply_solutions(raw_ms, sol_path)
                if sol_path else raw_ms
            )

            cube, times = process_single_obs(obs_id, ms_in, work_root)
            cubes.append(cube)
            axes.append(times)

        # combine and animate
        stack = np.concatenate(cubes)
        all_times = Time(
            np.concatenate([t.jd for t in axes]),
            format="jd", scale="utc"
        )
        order = np.argsort(all_times.jd)
        video_path = out_base / f"{flare_id}_{tag}_stokes_i.mp4"
        imaging.animate_stack(stack[order], all_times[order], video_path)
        log.info("finished → %s", video_path)
    finally:
        shutil.rmtree(work_base, ignore_errors=True)
        for d in tmp_dirs:
            shutil.rmtree(d, ignore_errors=True)


def fetch_ms(obs_id: str):
    """copy an mwa measurement set to a temp dir and return (ms_path, tmp_dir)"""
    ms_files, tmp_dir = get_ms_files(get_observation_path(obs_id))
    return Path(tmp_dir) / ms_files[0].name, tmp_dir


def process_single_obs(obs_id: str, ms_path: Path, work_root: Path):
    """run wsclean on one ms and return (cube, time_axis)"""
    start, dt, n = imaging.get_time_info(ms_path)
    log.info(f"{obs_id}: {n} intervals of {dt.sec:.1f}s starting {start.iso}")

    obs_dir = work_root / obs_id
    imaging.run_wsclean(ms_path, n, obs_dir)
    cube  = imaging.load_stokes_i_stack(obs_dir)
    times = start + np.arange(len(cube)) * dt
    shutil.rmtree(obs_dir, ignore_errors=True)
    return cube, times


if __name__ == "__main__":
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_base.mkdir(parents=True, exist_ok=True)

    for idx, cfg in enumerate(runs, 1):
        tag = f"{idx}_{timestamp}"
        log.info("starting job %d / %d (tag %s)", idx, len(runs), tag)
        run_job(cfg, tag)

    log.info("all jobs completed.")
