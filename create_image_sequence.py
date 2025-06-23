from casacore.tables import table
from astropy.time import Time, TimeDelta
from astropy.io import fits
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
import matplotlib
import subprocess
import shutil
import os
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


# --- constants and paths ---
OBSERVATION = "1126847624"
OBSERVATION_PATH = Path("/mnt/nas05/data02/tschai/1126847624_basic_calibration/1126847624_ch187-188.ms")
IMAGE_SIZE_PIXELS = 2048
SCALE_ARCSEC_PER_PIXEL = 5
DAY_SECONDS = 86400
WORK_DIR = Path.cwd() / "tmp"
RAW_DIR = WORK_DIR / "raw"
MERGED_DIR = WORK_DIR / "merged"
FINAL_VIDEO_PATH = Path.cwd() / "vids" / f"{OBSERVATION}_stokes_i.mp4"


def main():
    start_time, interval_size, interval_count = get_time_info(OBSERVATION_PATH)
    logging.info(f"time range: {start_time.iso} to {(start_time + interval_count * interval_size).iso}")
    logging.info(f"intervals: {interval_count} x {interval_size.sec:.1f} s")

    """

    run_wsclean(OBSERVATION_PATH, interval_count)
    merge_stokes_i(interval_count)
    """
    stack = load_stokes_i_stack()
    logging.info(f"stack shape: {stack.shape}")
    animate_stack(stack, start_time, interval_size, FINAL_VIDEO_PATH)


def get_time_info(obs_path: Path):
     # function to get observation time metadata
    observation = table(str(obs_path))
    observation.unlock()

    intervals_center = observation.getcol('TIME')
    intervals_size = observation.getcol('INTERVAL')
    radius = intervals_size / 2.0

    start_mjd = np.min(intervals_center - radius) / DAY_SECONDS
    end_mjd = np.max(intervals_center + radius) / DAY_SECONDS

    start_time = Time(start_mjd, format='mjd', scale='utc')
    end_time = Time(end_mjd, format='mjd', scale='utc')
    interval_size = TimeDelta(intervals_size[0], format='sec')

    assert np.all(intervals_size == intervals_size[0])
    interval_count = int((end_time - start_time) / interval_size)

    return start_time, interval_size, interval_count


def run_wsclean(obs_path: Path, interval_count: int):
     # make sure the raw directory is clean
    reset_dir(RAW_DIR)

    cmd = [
        "wsclean",
        "-intervals-out", str(interval_count),
        "-size", str(IMAGE_SIZE_PIXELS), str(IMAGE_SIZE_PIXELS),
        "-scale", f"{SCALE_ARCSEC_PER_PIXEL}asec",
        "-niter", "0",
        "-pol", "xx,yy",          # two pols so join makes sense
        "-join-polarizations",
        "-channels-out", "1",
        str(obs_path),
    ]

    # inherit current env but pin openblas to one thread
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1")

    try:
        res = subprocess.run(
            cmd, cwd=RAW_DIR, env=env, check=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        logging.info("wsclean output:\n" + res.stdout)
    except subprocess.CalledProcessError as exc:
        logging.error("wsclean failed:\n" + exc.stdout)
        raise


def reset_dir(path: Path):
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def merge_stokes_i(interval_count: int):
     # merge polarizations into stokes i
    reset_dir(MERGED_DIR)
    for i in range(interval_count):
        prefix = f"wsclean-t{i:04d}"
        xx_file = RAW_DIR / f"{prefix}-XX-image.fits"
        yy_file = RAW_DIR / f"{prefix}-YY-image.fits"
        stokes_i_file = MERGED_DIR / f"{prefix}-I-image.fits"

        if not (xx_file.exists() and yy_file.exists()):
            logging.info(f"Skipping interval {i} due to missing XX or YY image.")
            continue

        with fits.open(xx_file) as hdul_xx, fits.open(yy_file) as hdul_yy:
            data_xx = np.squeeze(hdul_xx[0].data)
            data_yy = np.squeeze(hdul_yy[0].data)
            header = hdul_xx[0].header

        data_i = 0.5 * (data_xx + data_yy)

        header['BTYPE'] = 'Stokes I'
        header['NAXIS'] = 2

        prefixes = ['CTYPE', 'CRVAL', 'CRPIX', 'CDELT', 'CUNIT', 'NAXIS']
        for key in list(header):
            for prefix in prefixes:
                if key.startswith(prefix) and len(key) > len(prefix):
                    try:
                        axis_num = int(key[len(prefix):])
                        if axis_num > 2:
                            del header[key]
                            break
                    except ValueError:
                        continue

        fits.writeto(stokes_i_file, data_i, header, overwrite=True)


def load_stokes_i_stack():
    files = sorted(MERGED_DIR.glob("wsclean-t*-I-image.fits"))
    return np.array([np.squeeze(fits.getdata(f)) for f in files])


def animate_stack(stack: np.ndarray, start_time: Time, interval_size: TimeDelta, output_path: Path):
    FINAL_VIDEO_PATH.parent.mkdir(parents=True, exist_ok=True)

    matplotlib.rcParams['animation.embed_limit'] = 2000
    fig, ax = plt.subplots()

    extent = [-IMAGE_SIZE_PIXELS * SCALE_ARCSEC_PER_PIXEL / 2.0,
               IMAGE_SIZE_PIXELS * SCALE_ARCSEC_PER_PIXEL / 2.0,
              -IMAGE_SIZE_PIXELS * SCALE_ARCSEC_PER_PIXEL / 2.0,
               IMAGE_SIZE_PIXELS * SCALE_ARCSEC_PER_PIXEL / 2.0]

    img = ax.imshow(stack[0], cmap='gray', origin='lower', extent=extent)
    title_obj = ax.set_title(f"Solar Radio Emission (Stokes I) - Frame 0\n{start_time.iso}")
    ax.set_xlabel("X (arcsec)")
    ax.set_ylabel("Y (arcsec)")

    def update(frame):
        frame_data = stack[frame]
        img.set_data(frame_data)
        img.set_clim(vmin=frame_data.min(), vmax=frame_data.max())
        time_label = (start_time + frame * interval_size).iso
        title_obj.set_text(f"Solar Radio Emission (Stokes I) - Frame {frame}\n{time_label}")
        return [img, title_obj]

    ani = animation.FuncAnimation(fig, update, frames=stack.shape[0], interval=50, blit=True)
    ani.save(output_path, writer=FFMpegWriter(fps=20, bitrate=1800), dpi=150)
    logging.info(f"animation saved to: {output_path}")

     # clean up temporary directories
    shutil.rmtree(WORK_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()