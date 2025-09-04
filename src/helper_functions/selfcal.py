import logging, subprocess, shutil, tarfile
from pathlib import Path
import numpy as np

from astropy.io import fits
from astropy.wcs import WCS
from astropy.time import Time
from astropy.coordinates import SkyCoord, GCRS, get_sun
import astropy.units as u

from scipy import ndimage as ndi
from scipy.optimize import least_squares
import matplotlib.pyplot as plt
from matplotlib import animation
from helper_functions.utils import get_time_info, ms_central_frequency, _extract_metafits_from_tar
import helper_functions.mwa_imaging as imaging

logging.basicConfig(level=logging.INFO, format="%(message)s")
TMP_DIR = Path("/mnt/nas05/data02/predrag/data/mwa_data/tmp")


def self_calibrate(ms_in: Path, work_root: Path, iterations: int = 2,
                   wsclean_niter: int = 10, auto_threshold: float = 5.0) -> Path:
    """
    run a simple phase-only self-calibration loop:
      1) shallow wsclean per-interval imaging
      2) robust flare localization per frame → median ra/dec
      3) write one-point sky model
      4) solve and apply gains
    returns final self-calibrated ms path
    """
    work_root = Path(work_root); work_root.mkdir(parents=True, exist_ok=True)
    current_ms = Path(ms_in)

    start, dt, n = get_time_info(current_ms)

    for it in range(1, iterations + 1):
        it_dir = work_root / f"selfcal_iter{it:02d}"
        it_dir.mkdir(parents=True, exist_ok=True)

        run_wsclean(current_ms, n, it_dir, wsclean_niter)

        (best_ra, best_dec), results = find_burst_position(it_dir)
        
        srclist = it_dir / f"selfcal_iter{it:02d}.yaml"
        write_point_srclist(
            results, srclist,
            score_threshold=30.0, max_sources=4,
            ref_freq=ms_central_frequency(current_ms),
            flux_norm=500.0
        )

        sol_path = it_dir / f"selfcal_iter{it:02d}_sols.fits"
        obs_id = current_ms.name.split("_")[0]
        metafits = _extract_metafits_from_tar(
            "/mnt/nas05/data02/predrag/data/mwa_data", 
            obs_id
        )

        subprocess.run(
            [
                "hyperdrive","di-calibrate",
                "-d", str(current_ms), str(metafits),
                "-s", str(srclist),
                "-o", str(sol_path),
                "--min-uv-lambda", "0"
            ],
            check=True
        )

        next_ms = current_ms.with_name(f"{current_ms.stem}_selfcal{it}{current_ms.suffix}")
        if next_ms.exists(): shutil.rmtree(next_ms)
        subprocess.run(
            ["hyperdrive","solutions-apply","-d",str(current_ms),str(metafits),
             "-s",str(sol_path),"-o",str(next_ms)],
            check=True
        )
        current_ms = next_ms

    return current_ms


def find_burst_position(work_dir: Path,
                        glob_pattern: str = "wsclean-*image.fits",
                        z_thresh: float = 5.0,
                        min_pixels: int = 9,
                        smooth_sigma: float = 1.0,
                        bg_sigma: float = 8.0,
                        peak_half_width: int = 4,
                        exclude_limb_px: int = 8,
                        debug: bool = True,
                        debug_dir: Path = "/mnt/nas05/clusterdata01/home2/predrag/STIX-MWA/results/plots/coords",
                        debug_max_frames: int = 6) -> tuple[float, float]:
    """
    detect a transient burst by temporal background removal + spatial high-pass,
    then pick a compact local maximum inside the solar disk (limb excluded).
    returns (ra_deg, dec_deg)

    debug mode writes per-step figures and prints per-frame diagnostics.
    """

    # helper: safe time string from fits header (for logging/fig titles)
    def _header_time_str(hdr):
        # try date-obs, else mjd-obs; return short iso string
        tstr = None
        if "DATE-OBS" in hdr:
            tstr = str(hdr["DATE-OBS"])
        elif "MJD-OBS" in hdr:
            try:
                tstr = Time(float(hdr["MJD-OBS"]), format="mjd").isot
            except Exception:
                tstr = None
        return tstr or "n/a"

    # collect frames
    fpaths = sorted(Path(work_dir).glob(glob_pattern))
    if not fpaths:
        raise FileNotFoundError(f"no fits found in {work_dir} matching {glob_pattern}")

    frames = []
    times = []
    wcs_ref = None
    for p in fpaths:
        with fits.open(p, memmap=False) as hdul:
            img = np.asarray(hdul[0].data, dtype=np.float32)
            img = np.squeeze(img)  # handle shapes like [1,y,x] or [pol,y,x]
            frames.append(img)
            times.append(_header_time_str(hdul[0].header))
            if wcs_ref is None:
                wcs_ref = WCS(hdul[0].header)

    cube = np.stack(frames, axis=0)  # [t, y, x]
    ny, nx = cube.shape[1:]

    # optional light smoothing to suppress pixel noise
    if smooth_sigma and smooth_sigma > 0:
        cube = ndi.gaussian_filter(cube, sigma=(0, smooth_sigma, smooth_sigma))

    # temporal median as quiet-sun background
    med = np.median(cube, axis=0)

    # build solar disk mask from median, keep largest blob, then erode to avoid limb
    thr = np.percentile(med, 85)
    disk = med > thr
    disk = ndi.binary_opening(disk, iterations=2)
    disk = ndi.binary_closing(disk, iterations=2)
    labels, nlab = ndi.label(disk)
    if nlab == 0:
        disk = np.ones_like(med, dtype=bool)
    elif nlab > 1:
        sizes = ndi.sum(disk, labels, index=np.arange(1, nlab + 1))
        keep = 1 + int(np.argmax(sizes))
        disk = labels == keep
    if exclude_limb_px > 0:
        disk = ndi.binary_erosion(disk, iterations=exclude_limb_px)

    # precompute a thin border of the disk for overlays
    disk_border = disk ^ ndi.binary_erosion(disk, iterations=1)

    # helper: robust sigma inside mask
    def robust_sigma(a, mask):
        vals = a[mask]
        if vals.size == 0:
            return 1.0
        m = np.median(vals)
        mad = np.median(np.abs(vals - m))
        s = 1.4826 * mad
        if not np.isfinite(s) or s <= 0:
            s = np.std(vals) if np.std(vals) > 0 else 1.0
        return float(s)

    # debug dir
    if debug:
        debug_dir = Path(debug_dir or (Path(work_dir) / "debug_find_flare"))
        debug_dir.mkdir(parents=True, exist_ok=True)
        # save median + mask overview
        fig, ax = plt.subplots(1, 3, figsize=(12, 3.5), constrained_layout=True)
        im0 = ax[0].imshow(med, origin="lower")
        ax[0].set_title("temporal median")
        plt.colorbar(im0, ax=ax[0], fraction=0.046, pad=0.04)
        im1 = ax[1].imshow(disk, origin="lower", cmap="gray")
        ax[1].set_title(f"solar disk mask (thr p85, erode {exclude_limb_px}px)")
        plt.colorbar(im1, ax=ax[1], fraction=0.046, pad=0.04)
        # quick histogram for sanity
        ax[2].hist(med.ravel(), bins=200, log=True)
        ax[2].set_title("median histogram")
        fig.suptitle("debug: median + mask")
        fig.savefig(debug_dir / "median_and_mask.png", dpi=140)
        plt.close(fig)

    # scan frames and score candidates
    results = []  # list of dicts per frame

    win_r = max(2, peak_half_width * 2)  # centroid window radius in pixels

    for i, frame in enumerate(cube):
        resid = frame - med
        smooth_bg = ndi.gaussian_filter(resid, sigma=bg_sigma) if bg_sigma > 0 else 0.0
        hp = resid - smooth_bg

        s = robust_sigma(hp, disk)
        z = np.where(disk, hp / s, 0.0)

        # detect local maxima inside disk
        max_filt = ndi.maximum_filter(z, size=(2 * peak_half_width + 1))
        peaks = (z == max_filt) & (z > z_thresh) & disk
        ys, xs = np.where(peaks)

        candidates = []
        for y0, x0 in zip(ys, xs):
            y1 = max(0, y0 - win_r); y2 = min(ny, y0 + win_r + 1)
            x1 = max(0, x0 - win_r); x2 = min(nx, x0 + win_r + 1)
            w = np.maximum(hp[y1:y2, x1:x2], 0.0)
            if (w > 0).sum() < min_pixels or w.sum() <= 0:
                continue
            yy, xx = np.indices(w.shape)
            x_c = x1 + (w * xx).sum() / w.sum()
            y_c = y1 + (w * yy).sum() / w.sum()
            peak_z = float(z[y0, x0])
            flux_sum = float(w.sum())
            score = 3.0 * peak_z + np.log1p(flux_sum)
            candidates.append((score, peak_z, flux_sum, x_c, y_c))

        mode = "burst" if len(candidates) > 0 else "fallback"
        if candidates:
            candidates.sort(reverse=True, key=lambda t: t[0])
            score, peak_z, flux_sum, x_c, y_c = candidates[0]
        else:
            # fallback: max positive high-pass residual inside disk
            masked = np.where(disk, hp, -np.inf)
            if np.all(~np.isfinite(masked)):
                y_c, x_c = ny / 2.0, nx / 2.0
                peak_z, flux_sum, score = 0.0, 0.0, -np.inf
                mode = "fallback-center"
            else:
                y_c, x_c = np.unravel_index(np.nanargmax(masked), masked.shape)
                peak_z = float(z[int(y_c), int(x_c)])
                flux_sum = float(max(masked[int(y_c), int(x_c)], 0.0))
                score = peak_z

        # log per-frame summary (small and informative)
        logging.info(
            "frame %02d time=%s mode=%s peaks=%d peak_z=%.2f score=%.2f xy=(%.1f, %.1f)",
            i, times[i], mode, int(len(candidates)), float(peak_z), float(score), float(x_c), float(y_c)
        )

        results.append(dict(
            idx=i, time=times[i], mode=mode, peaks=len(candidates),
            peak_z=float(peak_z), score=float(score), x=float(x_c), y=float(y_c),
            s=float(s)
        ))

    # add ra/dec to each entry
    wcs_sky = wcs_ref.celestial
    for r in results:
        ra, dec = wcs_sky.all_pix2world(r["x"], r["y"], 0)
        r["ra"] = float(ra)
        r["dec"] = float(dec)

    # choose best frame; prefer burst over fallback on ties
    def _rank_key(d):
        return (0 if d["mode"].startswith("burst") else 1, -d["score"])
    best = sorted(results, key=_rank_key)[0]
    x_c, y_c = best["x"], best["y"]
    logging.info("picked frame %d (%s) score=%.2f @ (x=%.1f, y=%.1f) time=%s",
                 best["idx"], best["mode"], best["score"], x_c, y_c, best["time"])

    # per-frame debug plots (top k by score + the best)
    if debug:
        # pick frames to visualize
        top = sorted(results, key=lambda d: (-d["score"], d["mode"].startswith("burst") is False))[:debug_max_frames]
        want_idxs = sorted({best["idx"], *[d["idx"] for d in top]})

        for i in want_idxs:
            frame = cube[i]
            resid = frame - med
            smooth_bg = ndi.gaussian_filter(resid, sigma=bg_sigma) if bg_sigma > 0 else 0.0
            hp = resid - smooth_bg
            s = robust_sigma(hp, disk)
            z = np.where(disk, hp / s, 0.0)

            # peaks for overlay
            max_filt = ndi.maximum_filter(z, size=(2 * peak_half_width + 1))
            peaks = (z == max_filt) & (z > z_thresh) & disk
            py, px = np.where(peaks)

            # figure with 2x3 panels
            fig, axes = plt.subplots(2, 3, figsize=(13, 7), constrained_layout=True)
            ax = axes.ravel()

            im0 = ax[0].imshow(frame, origin="lower"); ax[0].set_title(f"frame {i} (raw)")
            plt.colorbar(im0, ax=ax[0], fraction=0.046, pad=0.04)

            im1 = ax[1].imshow(resid, origin="lower"); ax[1].set_title("residual (frame - median)")
            plt.colorbar(im1, ax=ax[1], fraction=0.046, pad=0.04)

            im2 = ax[2].imshow(hp, origin="lower"); ax[2].set_title(f"high-pass (σ={bg_sigma}px)")
            plt.colorbar(im2, ax=ax[2], fraction=0.046, pad=0.04)

            im3 = ax[3].imshow(z, origin="lower"); ax[3].set_title(f"z inside disk (σ_robust={s:.3g})")
            plt.colorbar(im3, ax=ax[3], fraction=0.046, pad=0.04)

            # overlay peaks and chosen centroid on z map
            ax[4].imshow(z, origin="lower"); ax[4].set_title("peaks + chosen centroid")
            if len(px):
                ax[4].scatter(px, py, s=20, facecolors="none", edgecolors="white", linewidths=1.0, label="peaks")
            ax[4].scatter(results[i]["x"], results[i]["y"], s=60, marker="x", linewidths=2.0, color="yellow", label="chosen")
            # draw disk border
            by, bx = np.where(disk_border)
            ax[4].scatter(bx, by, s=1, c="cyan", alpha=0.5, label="disk")
            ax[4].legend(loc="upper right", fontsize=8)

            # histogram of hp inside disk for sanity
            vals = hp[disk].ravel()
            ax[5].hist(vals, bins=200, log=True)
            ax[5].set_title("high-pass histogram (inside disk)")

            fig.suptitle(f"debug: frame {i}  time={times[i]}  mode={results[i]['mode']}  peak_z={results[i]['peak_z']:.2f}  score={results[i]['score']:.2f}")
            fig.savefig(debug_dir / f"frame_{i:02d}_debug.png", dpi=140)
            plt.close(fig)

    # pixel → sky using celestial sub-wcs
    wcs_sky = wcs_ref.celestial
    ra_deg, dec_deg = wcs_sky.all_pix2world(x_c, y_c, 0)
    logging.info("burst centroid (ra, dec): %.6f, %.6f deg", ra_deg, dec_deg)

    return (best["ra"], best["dec"]), results


def write_point_srclist(results, out_yaml: Path,
                        score_threshold: float = 50.0,
                        max_sources: int = 2,
                        ref_freq: float = 154e6,
                        flux_norm: float = 500.0):
    """
    write multiple burst detections as point sources into a hyperdrive yaml
    results: list of dicts with 'ra', 'dec', 'score'
    score_threshold: only include sources above this score
    max_sources: maximum number of sources to include
    flux_norm: flux density assigned to the strongest source (Jy)
    """
    # filter and sort
    bursts = [r for r in results if r["mode"] == "burst" and r["score"] >= score_threshold]
    bursts = sorted(bursts, key=lambda r: -r["score"])[:max_sources]
    if not bursts:
        raise RuntimeError("no burst sources above threshold")

    max_score = max(r["score"] for r in bursts)

    lines = ["calibrator:"]
    for j, r in enumerate(bursts, 1):
        flux = (r["score"] / max_score) * flux_norm
        lines.append(f"  - ra: {r['ra']:.6f}")
        lines.append(f"    dec: {r['dec']:.6f}")
        lines.append("    comp_type: point")
        lines.append("    flux_type:")
        lines.append("      list:")
        lines.append(f"        - freq: {ref_freq:.1f}")
        lines.append(f"          i: {flux:.2f}")

    Path(out_yaml).write_text("\n".join(lines))
    logging.info("wrote sky model (%d sources) → %s", len(bursts), out_yaml)

