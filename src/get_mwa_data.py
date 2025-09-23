import os
import re
import ast
import glob
import pandas as pd
from pathlib import Path
from typing import List
import find_flares_in_mwa
from helper_functions.utils import get_root_path_to_data
from helper_functions.mwa_asvo import create_jobs, process_jobs
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


root_path_to_data = get_root_path_to_data()

def main():
    """
    download mwa data based on provided observation ids or flares matched with mwa metadata.
    optionally, ensure that metafits exist for already-downloaded ms tars in your data dir.
    """

    # set this to true if you want to check for and download missing metafits
    download_missing_metafits = False
    if download_missing_metafits:
        ensure_metafits(ms_root=root_path_to_data)


    download_data = True
    if download_data:
        files_path = "../files"
        observation_ids = [1403258056, 1387107440, 1401789256]  # leave empty to use flare list below

        job_info = {
            'job_type': 'c',    # 'c' for .ms data download, 'v' for voltage (raw data), 'm' for metadata

            # used only for job_type 'c':
            'avg_time_res': 4, 
            'avg_freq_res': 160, 
            'apply_cal': False
            }

        if observation_ids:
            download_by_obs_ids(observation_ids, job_info)
        else:
            filename = "flares_recorded_by_mwa_test.csv"
            download_by_flare_overlap(filename, files_path, job_info, flare_range=(162, 172))


def download_by_obs_ids(observations, job_info):
    """
    downloads mwa data using a manual list of observation ids
    """
    download_mwa_data(observations, job_info)


def download_by_flare_overlap(filename, files_path, job_info, flare_range=None):
    """
    downloads mwa data using flares overlapping with mwa observation times
    """
    logging.info(flare_range)
    flarelist_path = os.path.join(files_path, filename)

    # auto-generate flare file if missing
    if not os.path.exists(flarelist_path):
        find_flares_in_mwa.main()

    flare_data = pd.read_csv(flarelist_path)

    for i, row in flare_data.iterrows():
        if flare_range is None or (flare_range[0] <= i < flare_range[1]):
            download_mwa_data(row, job_info, is_flare_row=True)


def download_mwa_data(obs_source, job_info, is_flare_row=False):
    """
    downloads mwa data based on observation ids or flare row
    - obs_source: list of obs_ids or a flare row with 'obs_ids' field
    - avg_time_res: time averaging resolution
    - avg_freq_res: frequency averaging resolution
    - is_flare_row: set to True if passing a flare row
    """
    
    if is_flare_row:
        obs_ids = ast.literal_eval(obs_source['obs_ids'])
        flare_id = obs_source.get('flare_id', 'unknown')
    else:
        obs_ids = obs_source
        flare_id = None

     # download only new observations, or same observations if size differs
    downloaded_obs = get_downloaded_obs_info(root_path_to_data)
    new_obs_ids = [obs_id for obs_id in obs_ids if obs_id not in downloaded_obs]

    if not new_obs_ids:
        if flare_id:
            logging.info(f"All observations for flare {flare_id} have already been downloaded.")
        else:
            logging.info(f"All observations {new_obs_ids} have already been downloaded.")
        return

    jobs_to_submit = create_jobs(new_obs_ids, job_info)
    logging.info(jobs_to_submit)

    if jobs_to_submit:
        if flare_id:
            logging.info(f"Submitting {len(jobs_to_submit)} jobs for flare {flare_id}.")
        else:
            logging.info(f"Submitting {len(jobs_to_submit)} jobs for observations {new_obs_ids}.")
        process_jobs(jobs_to_submit)


def get_downloaded_obs_info(root_path: str | Path):
    """
    Collect observation IDs and their file sizes from files only, ignoring subfolders.
    Returns a dictionary mapping obs_id -> file size in bytes.
    """
    downloaded_obs = []
    root = Path(root_path)

    for entry in root.iterdir():
        if not entry.is_file():
            continue

        prefix = entry.name.split("_", 1)[0]
        if prefix.isdigit():
            downloaded_obs.append(int(prefix))

    return downloaded_obs


def ensure_metafits(ms_root: Path = root_path_to_data) -> None:
    """
    ensure metafits tars are present for all obs ids that already have ms tars.
    downloads any missing ones using your downloader with job_type='m'.
    """
    ms_root.mkdir(parents=True, exist_ok=True)
    missing = collect_missing_metafits_obsids(ms_root)
    if not missing:
        logging.info("no missing metafits detected.")
        return
    logging.info("downloading %d missing metafits…", len(missing))
    download_by_obs_ids(missing, job_info={'job_type': 'm'})


_MS_RE = re.compile(r"^(?P<obs>\d{9,12})_\d+_ms\.tar$")  # handles 9–12 digit obs ids
def _extract_obs_id(filename: str) -> str | None:
    """
    pull obs id from names like '1126847624_846700_ms.tar'
    """
    m = _MS_RE.match(filename)
    return m.group("obs") if m else None


def collect_obs_ids_from_ms(ms_root: Path = root_path_to_data) -> List[str]:
    """
    list all unique obs ids present as '*_ms.tar' under ms_root (non recursive).
    """
    obs_ids: set[str] = set()
    for path in ms_root.glob("*_ms.tar"):
        obs = _extract_obs_id(path.name)
        if obs:
            obs_ids.add(obs)
        else:
            logging.debug("skipping non-matching file: %s", path.name)
    return sorted(obs_ids)


def metafits_exists(obs_id: str, ms_root: Path = root_path_to_data) -> bool:
    """
    check if there is at least one metafits tar like 'obsid_*_vis_meta.tar'
    """
    pattern = str(ms_root / f"{obs_id}_*_vis_meta.tar")
    return len(glob.glob(pattern)) > 0


def collect_missing_metafits_obsids(ms_root: Path = root_path_to_data) -> List[str]:
    """
    return obs ids that have ms tar present but no corresponding metafits tar.
    """
    obs_ids = collect_obs_ids_from_ms(ms_root)
    missing = [obs for obs in obs_ids if not metafits_exists(obs, ms_root)]
    logging.info("found %d obs ids, %d missing metafits", len(obs_ids), len(missing))
    if missing:
        logging.info("missing metafits for obs ids: %s", ", ".join(missing))
    return missing


if __name__ == "__main__":
    main()
