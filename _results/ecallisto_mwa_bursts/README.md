# e-Callisto bursts during archived MWA Sun observations

Made 2026-10-04 with `src/ecallisto_mwa_bursts.py` (Claude).

Inputs
- e-Callisto monthly burst lists 2021-01 to 2026-09 (18,481 entries), FHNW server
  `soleil80.cs.technik.fhnw.ch/solarradio/data/BurstLists/2010-yyyy_Monstein`.
  To end of 2024 by visual inspection (with types); from 2025 by the deARCE
  machine-learning pipeline (University of Alcalá), mostly without types (`---`),
  and station lists that can include night-side stations.
- MWA Sun observations from `mwa_stix_overlap.py mwa` (31,737 obs to 2026-10-02).

Results
- `burst_mwa_pairs.csv`: 1,080 bursts overlap MWA Sun obs; 836 with archived data.
- `burst_ranking.csv`: 549 bursts (≤ 60 min, archived MWA, ASSA data present),
  ranked by strength = min(ASSA_56, ASSA_57) peak, in raw e-Callisto digits
  (relative). `mwa_obs_at_peak` = MWA obs containing the ASSA peak time.
- `top_bursts.csv`: top 12, with MWA details and the STIX flare in progress
  (data center list, Earth times). The STIX flare is "in progress", not proven
  to be the source of the burst.
- `vet_top12.png`: ASSA_57 spectrograms of the top 12 (orange: MWA obs). All 12
  are solar bursts; 2024-12-21 has only a 16 s IPS snapshot, 2025-11-11 04:31 is weak.

Choice for imaging (André, 2026-10-04): 2024-12-25 04:48–04:52 UTC type II,
MWA 1419137296 (OA004, 240 s, 74–290 MHz) during a STIX M6 / GOES M5.0 flare at
(−88″, −297″). Download: `scripts/download_1419137296.sh`.

Method notes
- MRO (same site as MWA) is not used for the ranking: MRO_60 shows a ~1 min
  full-band calibration block at fixed times (e.g. 02:30:00–02:31:00) and
  MRO_59 is dominated by RFI. Both gave false peaks in earlier attempts.
- Coverage: only bursts with ASSA_56/57 data (Australian daytime) are ranked.

STIX timing (checked 2026-10-04): the STIX flare list times are at Solar
Orbiter, not Earth. For 778 Earth-visible M/X flares, GOES peak − STIX peak
grows with the light-time difference with slope 0.88 (≈1); ~80 s for shifts
< 50 s, ~340 s for shifts > 250 s. The data center light curves have an
Earth-time option (`ltc`); the flare list does not.
