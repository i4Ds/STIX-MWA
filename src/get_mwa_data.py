import os
import ast
import pandas as pd
import find_flares_in_mwa
from helper_functions.utils import get_root_path_to_data
from helper_functions.mwa_asvo import create_jobs, process_jobs
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
import inspect, websocket._abnf as _abnf

if 'skip_utf8_validation' not in inspect.signature(_abnf.ABNF.validate).parameters:
    # old websocket-client (<1.0) – wrap, but DON’T touch rsv1
    _orig = _abnf.ABNF.validate
    def _patched(self, *a, **kw):
        # preserve rsv1, only ignore the extra kwarg
        self.rsv2 = self.rsv3 = 0
        return _orig(self)
    _abnf.ABNF.validate = _patched


def main():
    """
    download mwa data based on provided observation ids or flares matched with mwa metadata.
    """

    download_observations_by_flare_id = False 

    if download_observations_by_flare_id:
        flare_csv = pd.read_csv('../files/flares_recorded_by_mwa_no_time_correction.csv')
        all_files = os.listdir('../results/plots/spectrograms_and_light_curves/time_not_corrected')
        underscore_files = [f for f in all_files if f.startswith('_')]

        for file in underscore_files:
            flare_id = file.split('_')[-1].split('.')[0]
            if flare_id != "2209300319":
                continue

            observation_ids = ast.literal_eval(flare_csv[flare_csv['flare_id'] == int(flare_id)]['obs_ids'].values[0])
            logging.info(f"Flare {flare_id} has {observation_ids} observations.")
    else:
        files_path = '../files'
        observation_ids = ['1385093776', '1385094016', '1385094256', '1385094496', '1385094736', '1385119816']  # set to [] to use flare list
        #observation_ids = ['1387506256', '1387506016', '1387539736']  # set to [] to use flare list
        # observation_ids = []

    path_to_data = get_root_path_to_data()
    job_type = 'c'  # 'c' for conversion, 'v' for voltage

    if observation_ids:
        download_by_obs_ids(observation_ids, job_type, path_to_data)
    else:
        download_by_flare_overlap(files_path, job_type, path_to_data, flare_range=(1400, 1500))


def download_by_obs_ids(observations, job_type, path_to_data):
    """
    downloads mwa data using a manual list of observation ids
    """
    download_mwa_data(observations, path_to_data, job_type)


def download_by_flare_overlap(files_path, job_type, path_to_data, flare_range=None):
    """
    downloads mwa data using flares overlapping with mwa observation times
    """
    for use_time_corrected in [True, False]:
        logging.info(flare_range)
        logging.info(use_time_corrected)
        filename = (
            "flares_recorded_by_mwa_with_time_correction.csv"
            if use_time_corrected else
            "flares_recorded_by_mwa_no_time_correction.csv"
        )
        flarelist_path = os.path.join(files_path, filename)

        # auto-generate flare file if missing
        if not os.path.exists(flarelist_path):
            find_flares_in_mwa.main()

        flare_data = pd.read_csv(flarelist_path)

        for i, row in flare_data.iterrows():
            if flare_range is None or (flare_range[0] <= i < flare_range[1]):
                download_mwa_data(row, path_to_data, job_type, is_flare_row=True)


def download_mwa_data(obs_source, path_to_data, job_type, avg_time_res=4, avg_freq_res=160, is_flare_row=False):
    """
    downloads mwa data based on observation ids or flare row
    - obs_source: list of obs_ids or a flare row with 'obs_ids' field
    - path_to_data: path where downloaded data are stored
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

    downloaded_ids = get_downloaded_obs_ids(path_to_data)
    new_obs_ids = [obs_id for obs_id in obs_ids if obs_id not in downloaded_ids]

    if not new_obs_ids:
        if flare_id:
            logging.info(f"All observations for flare {flare_id} have already been downloaded.")
        else:
            logging.info(f"All observations {new_obs_ids} have already been downloaded.")
        return

    jobs_to_submit = create_jobs(new_obs_ids, job_type, avg_time_res, avg_freq_res)

    if jobs_to_submit:
        if flare_id:
            logging.info(f"Submitting {len(jobs_to_submit)} jobs for flare {flare_id}.")
        else:
            logging.info(f"Submitting {len(jobs_to_submit)} jobs for observations {new_obs_ids}.")
        process_jobs(jobs_to_submit)


def get_downloaded_obs_ids(root_path):
    downloads = os.listdir(root_path)
    return {int(piece) for d in downloads for piece in [d.split("_")[0]] if piece} 


if __name__ == "__main__":
    main()
