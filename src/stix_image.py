"""
STIX CLEAN image of one flare interval, reprojected to the Earth view.

Times on the command line are at Earth; they are shifted by the Earth–Solar
Orbiter light-time difference before selecting STIX pixel data. The Earth-view
map assumes the emission lies in the plane through Sun centre perpendicular to
the Solar Orbiter line of sight (good when the views are nearly opposite, as on
2022-09-30, poor when they are close to 90° apart).

    python src/stix_image.py files/stix/solo_L1_stix-sci-xray-cpd_...fits \
        --start 2022-09-30T03:57:04 --end 2022-09-30T03:57:44 --energy 4 10
Environment: environment-stix-imaging.yml (stixpy, xrayvision).
"""
import argparse
import logging
from pathlib import Path

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
from astropy.constants import c
from astropy.coordinates import SkyCoord
from astropy.time import Time
from sunpy.coordinates import HeliographicStonyhurst, Helioprojective, PlanarScreen, get_earth
from sunpy.map import Map, make_fitswcs_header
from sunpy.time import TimeRange
from xrayvision.clean import vis_clean
from xrayvision.imaging import vis_to_image

from stixpy.calibration.visibility import calibrate_visibility, create_meta_pixels, create_visibility
from stixpy.coordinates.frames import STIXImaging
from stixpy.coordinates.transforms import get_hpc_info
from stixpy.map.stix import STIXMap  # noqa: F401  registers the STIX map source
from stixpy.product import Product

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("stix_image")

ISC_10_7 = [3, 20, 22, 16, 14, 32, 21, 26, 4, 24, 8, 28]
ISC_10_3 = ISC_10_7 + [15, 27, 31, 6, 30, 2, 25, 5, 23, 7, 29, 1]


def visibilities(cpd, time_range, energy_range, location):
    meta = create_meta_pixels(cpd, time_range=time_range, energy_range=energy_range,
                              flare_location=location, no_shadowing=True)
    vis = create_visibility(meta)
    return vis, meta


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cpd", type=Path)
    p.add_argument("--start", required=True, help="Earth UTC")
    p.add_argument("--end", required=True, help="Earth UTC")
    p.add_argument("--energy", nargs=2, type=float, default=[4, 10], metavar=("LO", "HI"), help="keV")
    p.add_argument("--pixel", type=float, default=2.0, help="arcsec")
    p.add_argument("--npix", type=int, default=129)
    p.add_argument("--earth-fov", type=float, default=300.0, help="half width of Earth-view map, arcsec")
    p.add_argument("--out", type=Path, default=Path("files/stix"))
    args = p.parse_args()

    t0e, t1e = Time(args.start), Time(args.end)
    _, solo_xyz, _ = get_hpc_info(t0e, t1e)
    solo = HeliographicStonyhurst(*solo_xyz, obstime=t0e, representation_type="cartesian")
    earth = get_earth(t0e)
    dt = ((earth.radius - solo.spherical.distance) / c).to(u.s)
    t0s, t1s = t0e - dt, t1e - dt
    log.info(f"light-time Earth−SolO {dt:.1f}; STIX interval {t0s.isot} – {t1s.isot}")
    time_range = [t0s.isot, t1s.isot]
    energy = args.energy * u.keV
    cpd = Product(str(args.cpd))

    # Pass 1: full-disk back projection with coarse detectors to find the flare.
    vis, _ = visibilities(cpd, time_range, energy, [0, 0] * u.arcsec)
    vis_tr = TimeRange(vis.meta["time_range"])
    roll, solo_xyz, _ = get_hpc_info(vis_tr.start, vis_tr.end)
    solo = HeliographicStonyhurst(*solo_xyz, obstime=vis_tr.center, representation_type="cartesian")
    stix_frame = STIXImaging(obstime=vis_tr.start, obstime_end=vis_tr.end, observer=solo)
    centre = SkyCoord(0 * u.deg, 0 * u.deg, frame=stix_frame)
    cal = calibrate_visibility(vis, flare_location=centre)
    sel = cal[np.argwhere(np.isin(cal.meta["isc"], ISC_10_7)).ravel()]
    bp = vis_to_image(sel, [512, 512] * u.pixel, pixel_size=[10, 10] * u.arcsec / u.pixel)
    bp_map = Map((bp, make_fitswcs_header(bp, centre, scale=[10, 10] * u.arcsec / u.pix,
                                          telescope="STIX", observatory="Solar Orbiter")))
    iy, ix = np.unravel_index(np.nanargmax(bp_map.data), bp_map.data.shape)
    flare = bp_map.pixel_to_world(ix * u.pix, iy * u.pix)
    log.info(f"back-projection peak (STIX frame): {flare.Tx.to(u.arcsec):.0f}, {flare.Ty.to(u.arcsec):.0f}")

    # Pass 2: CLEAN around the flare with detectors 10–3.
    vis, _ = visibilities(cpd, time_range, energy, flare)
    cal = calibrate_visibility(vis, flare_location=flare)
    cal.meta["offset"] = flare
    sel = cal[np.argwhere(np.isin(cal.meta["isc"], ISC_10_3)).ravel()]
    shape = [args.npix, args.npix] * u.pixel
    pixel = [args.pixel, args.pixel] * u.arcsec / u.pixel
    clean, _, _ = vis_clean(sel, shape, pixel_size=pixel, gain=0.1, niter=200,
                            clean_beam_width=20 * u.arcsec)
    hpc_solo = Helioprojective(obstime=vis_tr.center, observer=solo)
    header = make_fitswcs_header(clean.data, flare.transform_to(hpc_solo), scale=pixel,
                                 rotation_angle=90 * u.deg + roll,
                                 telescope="STIX", observatory="Solar Orbiter")
    solo_map = Map((np.asarray(clean.data), header)).rotate()

    # Earth view: emission on the plane through Sun centre facing Solar Orbiter.
    hpc_earth = Helioprojective(obstime=vis_tr.center, observer=get_earth(vis_tr.center))
    with PlanarScreen(solo, distance_from_center=0 * u.m):
        flare_e = flare.transform_to(hpc_earth)
        n = int(round(2 * args.earth_fov / args.pixel))
        earth_header = make_fitswcs_header((n, n), flare_e, scale=pixel,
                                           telescope="STIX", observatory="Solar Orbiter (Earth view)")
        earth_map = solo_map.reproject_to(earth_header)
    log.info(f"flare in Earth view: {flare_e.Tx.to(u.arcsec):.0f}, {flare_e.Ty.to(u.arcsec):.0f}")

    tag = f"{t0e.strftime('%Y%m%dT%H%M%S')}_{args.energy[0]:g}-{args.energy[1]:g}keV"
    args.out.mkdir(parents=True, exist_ok=True)
    solo_map.save(args.out / f"stix_clean_{tag}_solo.fits", overwrite=True)
    earth_map.save(args.out / f"stix_clean_{tag}_earth.fits", overwrite=True)

    fig = plt.figure(figsize=(15, 5))
    for i, (m, title) in enumerate([(bp_map, "back projection, STIX frame"),
                                    (solo_map, "CLEAN, Solar Orbiter view"),
                                    (earth_map, "CLEAN, Earth view (plane-of-sky)")]):
        ax = fig.add_subplot(1, 3, i + 1, projection=m)
        m.plot(axes=ax, cmap="hot")
        m.draw_limb(axes=ax, color="w")
        ax.set_title(title)
    fig.suptitle(f"STIX {args.energy[0]:g}–{args.energy[1]:g} keV, Earth {t0e.isot[11:19]}–{t1e.isot[11:19]}")
    fig.tight_layout()
    png = args.out / f"stix_clean_{tag}.png"
    fig.savefig(png, dpi=110)
    log.info(f"wrote {png}")


if __name__ == "__main__":
    main()
