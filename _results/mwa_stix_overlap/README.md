# STIX flares during MWA Sun observations

Made 2026-10-02 with `src/mwa_stix_overlap.py` (Claude).

Inputs
- STIX: `STIX_flarelist_w_locations_20210214_20260228_version1_python.csv`
  from github.com/hayesla/stix_flarelist_science (33,076 flares, 2021-02-14 to 2026-02-28).
- MWA: ASVO TAP `mwa.observation`, 2021-02-01 to 2026-03-02, `sun_elevation > 0`,
  not deleted, pointing centre within 15° of the Sun or obsname contains sun/solar
  (28,292 obs; separations agree with the server's `sun_pointing_distance` to < 0.01°).

Method
- STIX times shifted to Earth arrival by (d_Earth − d_SolO)/c, −15 to +355 s.
  Note: the list's `EAR_TDEL` column is the Solar Orbiter–Earth distance / c
  (checked, r = 0.9999), not the Sun-photon delay, so it is not used.
- Flare interval padded by 60 s; any time overlap with an MWA obs counts.

Files
- `stix_flares_with_mwa.csv`: one row per flare (4,354 flares), with MWA obs ids,
  projects, overlap seconds, frequency range, archive status.
- `best_candidates_M_X_earth_archived.csv`: 123 flares, estimated GOES M/X,
  visible from Earth, at least one MWA obs with archived data.
- The full flare × obs pair table (12,010 rows, 8 MB) is not committed;
  rerun `python src/mwa_stix_overlap.py match ...` to make it.

Caveats
- G0060 (IPS survey) obs are 8–16 s snapshots near, not at, the Sun.
- STIX flare locations for summer 2025 have a known imaging issue (list README).
- GOES class is the list's STIX-based estimate (`goes_estimated_mean_class`).

## Full STIX data center list (2026-10-02)

`stix_dc_flares_with_mwa.csv`: same matching, but with the full flare list
from the STIX data center (`python src/mwa_stix_overlap.py stixdc`,
104,743 flares, 2021-02-01 to 2026-10-01) and MWA obs up to 2026-10-02
(31,737 Sun obs). Earth shift from the data center ephemeris
(`light_time_diff`, SPICE); agrees with the Hayes-based formula to < 0.4 s.
No flare locations yet; `cfl_x/y_arcsec` is the data center's CFL location
where it exists. Column `in_hayes_list` marks flares also in the Hayes list.

Comparison, up to 2026-02-28:
- Data center 93,892 flares; Hayes 33,076, all of them in the data center
  list with identical ids and times. Hayes appears to keep flares with
  4–10 keV QL peak ≥ ~1,000 counts/4 s (Hayes minimum 1,076; 99 % of the
  rest are below 991). Inferred from the data, not from the list docs.
- With MWA Sun obs: 8,748 flares (data center) vs 4,354 (Hayes). All 4,354
  Hayes flares are in the data center result; the extra 4,394 are weak
  (3,761 est. GOES B, 633 without estimate; median 431 vs 3,967 counts).
- After 2026-02-28 (data center only): 1,119 more flares, 20 est. M class.
  Caution: `goes_est_class` is from STIX counts; where the real GOES class
  (`goes_class_at_flare`) is much lower, the flare is probably not visible
  from Earth (not checked yet).
