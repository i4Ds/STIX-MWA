"""
compares MWA radio burst location with STIX HXR flare centroid
stix flare data from https://github.com/hayesla/stix_flarelist_science
mwa burst localization done via shallow imaging with WSCLEAN (and preforms calibration before)
"""
from pathlib import Path
import logging, shutil, subprocess, csv
import numpy as np
import matplotlib.pyplot as plt

import astropy.units as u
from astropy.io import fits
from astropy.time import Time
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from sunpy.coordinates import frames, get_earth, sun
from scipy.ndimage import gaussian_filter

from helper_functions.utils import get_observation_path, get_ms_files, get_root_path_to_data
import helper_functions.calibration as cal
from helper_functions.mwa_imaging import run_wsclean
from helper_functions.selfcal import find_burst_position

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("mwa_stix_compare")


# ============ user configuration =============
RUN = {
    "flare_id":       "2104122000",
    "observation_id": "1126847624_846700",
    "calibration_id": "1126832808_HydA_881096",   # set to None to skip external cal
    "calibrator_flux_jy": 500.0,
    "stix_csv_path":   "../files/STIX_flarelist_w_locations_20210214_20250228_version1_python.csv",
}
INTERVALS_OUT  = 24
SHALLOW_NITER  = 10
AUTO_THRESHOLD = 5.0
SMOOTH_SIGMA   = 1.0            # light display smoothing (px)
MWA_ERR_ARC    = 60.0           # ~ beam/snr 1σ (arcsec)
STIX_ERR_ARC   = 30.0           # stix centroid 1σ (arcsec)
KEEP_WORK      = False
# =============================================


def main():
    """
    run calibration, shallow imaging, localization, then plot with stix
    """
    root_path = get_root_path_to_data()
    tag = Time.now().utc.isot.replace(":", "").replace("-", "").split(".")[0]
    work_root = Path(root_path) / "tmp" / f"mwa_stix_{RUN['observation_id']}_{tag}"
    out_base  = Path.cwd().parent / "results" / "plots" / "positions_of_radio_and_hxr_flare"
    out_base.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)

    try:
        # external calibration (optional)
        sol_path = None
        if RUN.get("calibration_id"):
            sol_path = work_root / f"{RUN['flare_id']}_{tag}_cal_sols.fits"
            cal_ms  = get_ms_files(get_observation_path(RUN["calibration_id"]), work_root)
            cal.run_di_calibrate(cal_ms, RUN.get("calibrator_flux_jy", 500.0), sol_path, work_root)
            log.info(f"calibration solutions → {sol_path}")

        # science ms and apply solutions
        sci_ms = get_ms_files(get_observation_path(RUN["observation_id"]), work_root)
        sci_ms = cal.apply_solutions(sci_ms, sol_path, work_root) if sol_path else sci_ms
        sci_ms = Path(sci_ms)
        if check_ms_rows(sci_ms) == 0:
            raise RuntimeError(f"ms has zero rows after apply: {sci_ms}")

        # shallow imaging for localization
        img_dir = work_root / "frames_localize"
        img_dir.mkdir(parents=True, exist_ok=True)
        run_wsclean(
            sci_ms, img_dir,
            intervals_out=INTERVALS_OUT, niter=SHALLOW_NITER, auto_threshold=AUTO_THRESHOLD
        )

        # localize burst
        (ra_deg, dec_deg), results = find_burst_position(
            img_dir, glob_pattern="wsclean-*image.fits",
            z_thresh=5.0, min_pixels=9, smooth_sigma=1.0,
            bg_sigma=8.0, peak_half_width=4, exclude_limb_px=8, debug=True
        )
        best = best_result_entry(results)
        fits_list = sorted(img_dir.glob("wsclean-*image.fits"))
        if not fits_list:
            raise FileNotFoundError("no fits frames produced for localization")
        best_fits = fits_list[int(best["idx"])]
        log.info(f"picked frame {best['idx']} ({best['mode']}) score {best['score']:.2f}")

        # stix earth-view centroid from csv
        t_stix_mid, t_stix_peak, tx_stix, ty_stix = read_stix_earth_hpc(Path(RUN["stix_csv_path"]), RUN["flare_id"])

        # plot comparison panel
        out_png = out_base / f"{RUN['flare_id']}_mwa_vs_stix.png"
        t_mwa_iso = best.get("time", "n/a")
        plot_compare(best_fits, ra_deg, dec_deg, tx_stix, ty_stix, t_mwa_iso, t_stix_mid, t_stix_peak, out_png)

    finally:
        if not KEEP_WORK:
            shutil.rmtree(work_root, ignore_errors=True)
            log.info(f"cleaned temp dir: {work_root}")


def check_ms_rows(ms_path: Path) -> int:
    """
    return number of rows in the ms main table (0 on failure)
    """
    try:
        from casacore.tables import table
        with table(str(ms_path)) as T:
            return T.nrows()
    except Exception as e:
        log.error(f"failed to open ms {ms_path}: {e}")
        return 0


def best_result_entry(results: list[dict]) -> dict:
    """
    pick best frame: prefer 'burst*' mode, then highest score
    """
    return sorted(
        results,
        key=lambda d: (0 if str(d.get("mode","")).startswith("burst") else 1,
                       -float(d.get("score", -1e9)))
    )[0]


def read_stix_earth_hpc(csv_path: Path, flare_id: str):
    """
    read stix earth-view tx/ty [arcsec] and start/end/peak times from csv
    """
    with open(csv_path, newline="") as f:
        R = csv.DictReader(f)
        for row in R:
            if str(row["flare_id"]).strip() == str(flare_id):
                t0 = Time(row["start_UTC"].strip(), scale="utc")
                t1 = Time(row["end_UTC"].strip(),   scale="utc")
                tpk = row.get("peak_UTC", "").strip() or "n/a"
                tmid = (t0 + 0.5*(t1 - t0)).utc.isot
                tx = float(row["hpc_x_earth"])
                ty = float(row["hpc_y_earth"])
                return tmid, tpk, tx, ty
    raise ValueError(f"flare_id {flare_id} not found in {csv_path}")


def plot_compare(fits_path: Path, burst_ra_deg: float, burst_dec_deg: float,
                 stix_tx: float, stix_ty: float, t_mwa_iso: str,
                 t_stix_iso: str, t_stix_peak: str, out_png: Path):
    """
    plot radio frame with mwa burst (black x) and stix centroid (red +), annotate hpc and separation
    """
    # read image + header
    with fits.open(fits_path, memmap=False) as hdul:
        img = np.squeeze(hdul[0].data).astype(float)
        hdr = hdul[0].header

    if SMOOTH_SIGMA and SMOOTH_SIGMA > 0:
        img = gaussian_filter(img, sigma=SMOOTH_SIGMA)

    # wcs and obstime
    w2d = WCS(hdr).celestial
    obstime = Time(t_mwa_iso, scale="utc") if t_mwa_iso != "n/a" else (
        Time(hdr.get("DATE-OBS"), scale="utc") if hdr.get("DATE-OBS") else
        (Time(float(hdr["MJD-OBS"]), format="mjd", scale="utc") if "MJD-OBS" in hdr else Time.now())
    )
    dist = sun.earth_distance(obstime)
    hpc_earth = frames.Helioprojective(obstime=obstime, observer=get_earth(obstime))

    # mwa burst in pixel and true hpc
    c_icrs_mwa = SkyCoord(burst_ra_deg*u.deg, burst_dec_deg*u.deg, frame="icrs", obstime=obstime, distance=dist)
    bx, by = w2d.world_to_pixel(c_icrs_mwa)
    c_hpc_mwa = c_icrs_mwa.transform_to(hpc_earth)
    tx_mwa = float(c_hpc_mwa.Tx.to_value(u.arcsec))
    ty_mwa = float(c_hpc_mwa.Ty.to_value(u.arcsec))

    # stix earth-hpc to pixel using local linear scale around center
    ny, nx = img.shape; cx, cy = nx/2.0, ny/2.0
    cd = np.abs(np.array(w2d.wcs.cdelt)) * 3600.0
    arcsec_per_pix = float(np.nanmean(cd[:2])) if np.all(np.isfinite(cd[:2])) and np.nanmean(cd[:2])>0 else 10.0
    sx_pix = cx + stix_tx/arcsec_per_pix
    sy_pix = cy + stix_ty/arcsec_per_pix

    # separation and quick co-spatial flag
    sep_arc = float(np.hypot(tx_mwa - stix_tx, ty_mwa - stix_ty))
    sigma_tot = float(np.sqrt(MWA_ERR_ARC**2 + STIX_ERR_ARC**2))
    co_spatial = sep_arc <= 2.0 * sigma_tot

    # contrast
    lo, hi = np.nanpercentile(img, (1.0, 99.5))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        lo, hi = np.nanpercentile(img, (2.0, 98.0))

    # plot image
    fig, ax = plt.subplots(figsize=(8.6, 7.4))
    im = ax.imshow(img, origin="lower", cmap="viridis", vmin=lo, vmax=hi)

    # markers
    ax.plot(bx, by, marker="x", ms=11, mew=2.0, color="black", linestyle="none", label="radio burst")
    ax.plot(sx_pix, sy_pix, marker="+", ms=12, mew=2.0, color="red",   linestyle="none", label="hxr flare")

    # true hpc tick labels via sampling along mid row/col
    xticks = [t for t in ax.get_xticks() if 0 <= t <= nx-1]
    yticks = [t for t in ax.get_yticks() if 0 <= t <= ny-1]
    ax.set_xticks(xticks); ax.set_yticks(yticks)
    def _fmt(v): return f"{v:.0f}″"
    xlabels, ylabels = [], []
    for xt in xticks:
        c_icrs = w2d.pixel_to_world(np.asarray(xt), np.asarray(ny/2)).icrs
        c_hpc  = SkyCoord(c_icrs.ra, c_icrs.dec, frame="icrs", obstime=obstime, distance=dist).transform_to(hpc_earth)
        xlabels.append(_fmt(float(c_hpc.Tx.to_value(u.arcsec))))
    for yt in yticks:
        c_icrs = w2d.pixel_to_world(np.asarray(nx/2), np.asarray(yt)).icrs
        c_hpc  = SkyCoord(c_icrs.ra, c_icrs.dec, frame="icrs", obstime=obstime, distance=dist).transform_to(hpc_earth)
        ylabels.append(_fmt(float(c_hpc.Ty.to_value(u.arcsec))))
    ax.set_xticklabels(xlabels); ax.set_yticklabels(ylabels)
    ax.set_xlabel("helioprojective longitude (solar-x) [arcsec]")
    ax.set_ylabel("helioprojective latitude (solar-y) [arcsec]")

    # title + legend box with key numbers
    f0 = hdr.get("CRVAL3") or hdr.get("FREQ") or hdr.get("RESTFRQ")
    ftxt = f"{float(f0)/1e6:.1f} mhz" if f0 not in (None, "") else "frequency n/a"
    ax.set_title(f"mwa {obstime.utc.isot} | stix {t_stix_iso}", fontsize=11)

    leg = (
        f"mwa: tx={tx_mwa:.1f}″, ty={ty_mwa:.1f}″\n"
        f"stix: tx={stix_tx:.1f}″, ty={stix_ty:.1f}″\n"
        f"sep={sep_arc:.1f}″  (σ_tot≈{sigma_tot:.1f}″)  → {'co-spatial' if co_spatial else 'offset'}"
    )
    ax.legend(loc="upper left", frameon=True, fontsize=9)
    ax.text(0.98, 0.02, leg, transform=ax.transAxes, ha="right", va="bottom",
            fontsize=9, color="white", bbox=dict(facecolor="black", alpha=0.6, edgecolor="white"))

    # colorbar
    cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    bunit = (str(hdr.get("BUNIT","")).strip() or "arb.").lower()
    cb.set_label("jy/beam" if str(hdr.get("BUNIT","")).strip().upper()=="JY/BEAM" else bunit)

    plt.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)
    log.info(f"saved → {out_png}")


if __name__ == "__main__":
    main()
