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
