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
        observation_ids = []  # leave empty to use flare list below
  
         # calibration observations: 'avg_time_res': 4, 'avg_freq_res': 160 
        observation_ids = [1403258056, 1387107440, 1401702856, 1401789256, 1387485312, 
        1384860616, 1387053016, 1374314416, 1348574416, 1374274216, 
        1385065816, 1387830912, 1387139416, 1384893016, 
        1387485016, 1347139816, 1401012016, 1408226600, 1348398016, 
        1406585000, 1400839216, 1410172160, 1403171656,
        1415789048, 1415789184, 1415789376, 1415789392, 1387453040, 1387453336, 
        1348176016, 1348176496, 1348176736, 1413801224, 1413801240, 1413801264, 
        1400407216, 1400493616, 1413428352, 1416393888, 1416393904, 1416393920, 1416394056, 
        1416394080, 1416394232, 1416394256, 1416394272, 1416394408, 1416394424, 1416394448, 1423625024, 
        1423625416, 1423625632, 1415183624, 1415183640, 1415183656, 1415183792, 1415183816, 
        1415183832, 1421726032, 1421726632, 1421727232, 1421727840, 1415159920, 1421723616, 1421724000, 
        1421724224, 1421724240, 1421724824, 1421725424, 1420432752, 1421292976, 1421293576, 1421633608, 
        1421634208, 1416896728, 1424228632, 1416454720, 1421808832, 1421809432, 1421810032, 1421810640, 
        1421811368, 1424232248, 1424232856, 1402048456, 1416538704, 1416539312, 1424316832, 1414643328, 
        1414643928, 1424407440, 1424408040, 1424408640, 1422332400, 1422333000, 1416631928, 1416632592, 
        1421124888, 1424238840, 1424239440, 1416460936, 1424324664, 1424325264, 1417399424, 1417400024, 
        1417400624, 1417401232, 1405996160, 1346383816, 1346384112, 1423286736, 
        1423287336, 1423287944, 1423288544, 1423289144, 1406606536, 1387917312, 1416800000, 
        1416800600, 1424756728, 1424757336, 1424574240, 1423708024, 1423708216, 1423708624, 1424143408, 
        1424144016, 1421287152, 1421288008, 1406455456, 1421816184, 1421816784, 1421817392, 1421818120, 
        1421818720, 1400752816, 1424584424, 1424585032, 1424585696, 1424586360, 1412991400, 1412992104, 
        1416279800, 1408848832, 1420942440, 1424746440, 1417235592, 1417236192, 1424486656, 1424487816]
        """
         # for full data download: 'avg_freq_res': 160
        observation_ids = [
        1387849216, 1348540464, 1421562616, 1406599216, 1406599432, 1417057392, 1416890016, 
        1421729640, 1401684016,
        1387087464, 1387087704, 1401685816, 1401684736, 1401773176, 
        1401773296, 1348553784, 1348554080, 1387603224, 1387603464, 1401767176]
        """


        job_info = {
            'job_type': 'c',  # 'c' for conversion, 'v' for voltage, 'm' for metadata

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
    downloaded_info = get_downloaded_obs_info(root_path_to_data)
    new_obs_ids = [
        obs_id for obs_id, size in expected_obs.items()
        if obs_id not in downloaded_info or size not in downloaded_info[obs_id]
    ]

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


def get_downloaded_obs_info(root_path: str | Path) -> dict[int, int]:
    """
    Collect observation IDs and their file sizes from files only, ignoring subfolders.
    Returns a dictionary mapping obs_id -> file size in bytes.
    """
    obs_info: dict[int, int] = {}
    root = Path(root_path)

    for entry in root.iterdir():
        if not entry.is_file():
            continue

        prefix = entry.name.split("_", 1)[0]
        if prefix.isdigit():
            obs_id = int(prefix)
            size = entry.stat().st_size
            obs_info[obs_id] = size

    return obs_info


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
