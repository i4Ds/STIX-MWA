from pathlib import Path
import shutil, logging, datetime, os
import numpy as np
from astropy.time import Time
from helper_functions.utils import get_observation_path, get_ms_files, get_root_path_to_data
import helper_functions.mwa_imaging as imaging
import helper_functions.calibration as cal
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


runs = [
    {
        "flare_id":        "1126847624",
        "observation_ids": ['1126847624'],
        "calibration_id":  "1126832808",            # set to None for no calibration
        "calibrator_flux_jy": 500.0,                 # default 1670.0
    },
]
"""
runs = [
    {
        "flare_id":        "14_2312250224",
        "observation_ids": ['1387506016', '1387506256'],
        "calibration_id":  "1387485312",            # set to None for no calibration
        "calibrator_flux_jy": 500.0,                 # default 1670.0
    },
]
"""
print_ = 'in wsclean i included percentiles for cutting as well as cmd "-niter", "5000", "-auto-mask", "3","-auto-threshold","3","-weight", "briggs", "0", "-apply-primary-beam"'


root_path_to_data = get_root_path_to_data()
work_base = Path(root_path_to_data) / "tmp"
out_base  = Path.cwd().parent / "results" / "mwa_vids"


def run_job(cfg: dict, tag: str):
    """orchestrate one job – optional calibration plus imaging stack"""
    flare_id        = cfg["flare_id"]
    observation_ids = cfg["observation_ids"]
    calibration_id  = cfg["calibration_id"]
    flux_jy         = cfg["calibrator_flux_jy"]

    work_root = work_base / tag
    work_root.mkdir(parents=True, exist_ok=True)

    try:

        # optional calibration
        if calibration_id:
            sol_path = work_root / f"{tag}_cal_sols.fits"
            cal_ms = get_ms_files(get_observation_path(calibration_id), work_root)
            cal.run_di_calibrate(cal_ms, flux_jy, sol_path, work_root)
        else:
            sol_path = None

        # imaging for each science ms

        cubes, axes = [], []

        for obs_id in observation_ids:
            raw_ms = get_ms_files(get_observation_path(obs_id), work_root)
            ms_in = (
                cal.apply_solutions(raw_ms, sol_path, work_root)
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
        video_path = out_base / f"{flare_id}_{tag}.mp4"
        imaging.animate_stack(stack[order], all_times[order], video_path)
        log.info("finished → %s", video_path)
    finally:
        shutil.rmtree(work_root, ignore_errors=True)


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
        log.info(print_)
        for key, value in cfg.items():
            log.info("cfg %-15s : %s", key, value)   # %-15s left-aligns keys into a tidy column
        run_job(cfg, tag)

    log.info("all jobs completed.")
