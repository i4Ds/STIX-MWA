from pathlib import Path
import os, shutil, subprocess, logging, re
import numpy as np
from casacore.tables import table
from astropy.io import fits
from astropy.time import Time, TimeDelta
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
from helper_functions.utils import find_data_column, reset_dir

log = logging.getLogger(__name__)


def run_wsclean(ms_path: Path, interval_count: int, work_dir: Path, n_iter=10, image_size_pixels=2048, scale_arcsec_per_pixel=5):
    """run wsclean snapshot imaging"""
    reset_dir(work_dir)
    cmd = [
        "wsclean",
        "-data-column", find_data_column(ms_path),
        "-intervals-out", str(interval_count),
        "-size", str(image_size_pixels), str(image_size_pixels),
        "-scale", f"{scale_arcsec_per_pixel}asec",
        "-pol", "xx,yy",
        "-join-polarizations",

        "-niter", n_iter, 
        "-auto-mask", "3",
        "-auto-threshold", "0.7",
        "-multiscale",
        "-mgain", "0.8",
        "-weight", "briggs", "0",
        #"-apply-primary-beam",

        str(ms_path),
    ]

    env = dict(os.environ, OPENBLAS_NUM_THREADS="1")
    env["MWA_BEAM_FILE"] = str(Path.home() / "local/share/mwa_full_embedded_element_pattern.h5")

    res = subprocess.run(
        cmd, cwd=work_dir, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, check=True
    )
    log.info("wsclean output:\n%s", res.stdout)


def load_stokes_i_stack(work_dir: Path) -> np.ndarray:
    #read wsclean fits files into [t, y, x] cube
    files = sorted(work_dir.glob("wsclean-t*-image.fits"))
    if not files:
        raise FileNotFoundError("no stokes-i fits produced")
    stack = []
    for f in files:
        with fits.open(f, memmap=False) as hdul:
            stack.append(np.squeeze(hdul[0].data))
    return np.array(stack)


def animate_stack(stack: np.ndarray, times: Time, out_path: Path):
    """save stack as mp4 animation"""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots()
    half = image_size_pixels * scale_arcsec_per_pixel / 2.0
    extent = [-half, +half, -half, +half]

    img = ax.imshow(stack[0], cmap="gray", origin="lower", extent=extent)
    title = ax.set_title("")
    ax.set_xlabel("x (arcsec)")
    ax.set_ylabel("y (arcsec)")

    def update(frame):
        frame_data = stack[frame]
        img.set_data(frame_data)
        img.set_clim(vmin=frame_data.min(), vmax=frame_data.max())
        title.set_text(
            f"solar radio emission (stokes i) – frame {frame}\n{times[frame].iso}"
        )
        return img, title

    ani = animation.FuncAnimation(fig, update, frames=len(stack),
                                  interval=50, blit=True)
    ani.save(out_path, writer=FFMpegWriter(fps=20, bitrate=1800), dpi=150)
    log.info("animation saved → %s", out_path)
