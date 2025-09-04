"""
orchestrate calibration (optional) -> self-calibration (optional) -> wsclean -> imaging
"""
from pathlib import Path
import shutil, logging, datetime, os
import numpy as np
from astropy.time import Time
from helper_functions.utils import get_observation_path, get_ms_files, get_root_path_to_data, get_time_info
import helper_functions.mwa_imaging as imaging
import helper_functions.calibration as cal
from helper_functions.selfcal import self_calibrate
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


runs = [
    {
        "flare_id":        "1126847624",
        "observation_ids": ['1126847624_846700'],
        "calibration_id":  "1126832808_HydA_881096",           # set to None for no calibration
        "calibrator_flux_jy":  500.0,                          # flux of calibrator at 150MHz
        "selfcal":       False,
    },
] 

# default wsclean parameters
image_size_pixels = 2048
scale_arcsec_per_pixel = 5
niter = 10

root_path_to_data = get_root_path_to_data()
work_base = Path(root_path_to_data) / "tmp"
out_base  = Path.cwd().parent / "results" / "mwa_vids"


def main():
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_base.mkdir(parents=True, exist_ok=True)

    for idx, cfg in enumerate(runs, 1):
        tag = f"{idx}_{timestamp}"
        log.info("starting job %d / %d (tag %s)", idx, len(runs), tag)

        log.info("all jobs completed.")
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
                
                if cfg["selfcal"]:
                    # self-calibration loop using wsclean
                    ms_in = self_calibrate(
                        ms_in=ms_in,
                        work_root=Path(work_root),
                        iterations=1,
                        wsclean_niter=5,
                        auto_threshold=5.0,
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
    start, dt, n = get_time_info(ms_path)
    log.info(f"{obs_id}: {n} intervals of {dt.sec:.1f}s starting {start.iso}")

    obs_dir = work_root / obs_id
    imaging.run_wsclean(ms_path, n, obs_dir, niter, image_size_pixels, scale_arcsec_per_pixel)
    cube  = imaging.load_stokes_i_stack(obs_dir)
    times = start + np.arange(len(cube)) * dt
    shutil.rmtree(obs_dir, ignore_errors=True)
    return cube, times


if __name__ == "__main__":
    main()
