# e-Callisto bursts during archived MWA Sun observations

First made 2026-10-04, redone 2026-10-16 with `src/ecallisto_mwa_bursts.py`
(Claude). Plan item 7a/7b (umbrella `docs/sabbatical-plan.md`, issue #21).

Inputs
- e-Callisto monthly burst lists 2021-01 to 2026-10, FHNW server
  `soleil80.cs.technik.fhnw.ch/solarradio/data/BurstLists/2010-yyyy_Monstein`.
  To end of 2024 by visual inspection (with types); from 2025 by the deARCE
  machine-learning pipeline (University of Alcalá), mostly without types (`---`),
  and station lists that can include night-side stations.
- MWA Sun observations from `mwa_stix_overlap.py mwa` (33,686 obs, 2021-01 to
  2026-10-16; not committed, 15 MB, rerun to get it).
- `../my_mwa_obs/calculon_obsids.txt`: 2,551 MWA obsids already on calculon
  (`/mnt/nas05/data02/MWA_data/data/mwa_data{,2}`), listed 2026-10-15.
  Refresh with `ecallisto_mwa_bursts.py calculon` (needs the FHNW VPN).

Results (2026-10-16)
- `burst_mwa_pairs.csv`: 1,082 bursts overlap MWA Sun obs; 838 with archived data.
- `burst_ranking.csv`: 641 bursts (≤ 60 min, archived MWA, ASSA data present),
  ranked by strength in raw e-Callisto digits (relative). With ASSA_56 and
  ASSA_57 both present, strength = the smaller of the two; in the ASSA_60 era
  (to December 2023) it is ASSA_60 alone (`single_rx`). `mwa_obs_at_peak` = MWA
  obs containing the ASSA peak time.
- `candidates.csv`: one row per burst × MWA obs at the peak, 627 rows; tier A 12,
  B 33, C 518, E 64 (19 Oct). Tier E = not downloadable for us yet: still under
  the 18-month embargo and not a collaboration (G…) project. By the MWA Data
  Access Policy, open-access (OA…) data go to the PI's team only during the
  embargo; all 64 are OA004. Columns `embargoed`, `mwa_public_release`
  (from mwa.observation, cached in `obs_access.csv`, not committed). Tier A: strength ≥ 5, typed burst, ≥ 3 stations, MWA data
  archived, Good, ≥ 56 s long, peak ≥ 8 s inside the obs, Sun elevation ≥ 30°,
  and an archived calibrator with the same channel set within 24 h.
  `why_not_A` says what a B/C row misses. Order within a tier: Phase IIIb first,
  then on calculon, then strength. `phase`: the archive labels everything from
  2024-10-28 "Phase III" and has no IIIb label; all 256 tiles are correlated only
  from about April 2025 (2025B in the MWA wiki), so IIIb here = Phase III, start
  ≥ 2025-04-01, more than 128 good tiles. Phase IIIb: 157 rows (A 2, B 10, C 145),
  none on calculon.
- `pictures/`: one picture per tier A/B candidate and per Phase IIIb row with
  strength ≥ 3 (ASSA, ±10 min and ±60 s,
  background subtracted; orange = MWA obs and band; red = proposed channel,
  series channels, 4 s image time and 2 s series window).
  `pictures/proposals.csv` holds the proposals as numbers. Channels are chosen
  by mean ASSA S/N (excess / per-frequency MAD) over the series window; the 4 s
  image sits where the best channel peaks, at least 8 s inside the obs. Gallery for review:
  https://claude.ai/artifact/AK68oh8tXW15ZL1zSxU17d (private).
- Checks: 2022-09-30 04:28 (MWA 1348547272, the burst imaged so far) comes out
  tier A through ASSA_60. The 16 s obs of 2024-12-21 and the weak 2025-11-11
  04:31 burst are not tier A.
- Older, still valid as a record: `top_bursts.csv` and `vet_top12.png` (2026-10-04,
  ASSA_56/57 only).

Choice for imaging (André, 2026-10-04): 2024-12-25 04:48–04:52 UTC type II,
MWA 1419137296 (OA004, 240 s, 74–290 MHz, Phase III). Its MS and both
calibrators (1419109008, 1419161128) are on calculon in `mwa_data2`.

Method notes
- ASSA receivers in the MWA band: ASSA_60 (108–524 MHz) until December 2023,
  then ASSA_56 and ASSA_57 (108–377 MHz). ASSA_62/63 are 15–88 MHz and not used.
  The 2026-10-04 version used only ASSA_56/57 and so missed 2022–2023.
- MRO is Metsähovi, Finland, not the MWA site. It is not used: the Sun is low or
  down there during MWA hours (about 00–08 UTC), MRO_60 shows a ~1 min
  full-band calibration block at fixed times (e.g. 02:30:00–02:31:00) and
  MRO_59 is dominated by RFI. Both gave false peaks in earlier attempts.
- Coverage: only bursts with ASSA data (Australian daytime) are ranked.

STIX timing (checked 2026-10-04): the STIX flare list times are at Solar
Orbiter, not Earth. For 778 Earth-visible M/X flares, GOES peak − STIX peak
grows with the light-time difference with slope 0.88 (≈1); ~80 s for shifts
< 50 s, ~340 s for shifts > 250 s. The data center light curves have an
Earth-time option (`ltc`); the flare list does not.
