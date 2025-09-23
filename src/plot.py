"""
plots data for flares based on provided observation ids or from STIX flares csv matched.
plots:
    - stix light curves
    - mwa spectrograms 
    - mwa light curves
    - location of the stix instrument at a given time
    - eCALLISTO spectrograms if available
"""

import os
import gc
import logging 
import traceback
import matplotlib.pyplot as plt
from helper_functions.stix import get_flarelist
from helper_functions.plot_flare import plot_flare


def main():
    """
    either provide a list of observation ids, or a flare csv file will be used
    """
    save_folder = '../results/plots/spectrograms_and_light_curves_G0002_vfe_true'
    observations = ['1126847624']  # set to [] to use flare list
    observations = []

    if observations:
        plot_by_observations(observations, save_folder)
    else:
        flare_csv = "../files/flares_recorded_by_mwa_G0002_vfe_true.csv"
        plot_by_flarelist(save_folder, flare_csv, flare_range=(0, 250))  # None or e.g. flare_range=(0, 3000)


def plot_by_observations(observations, save_folder):
    """
    plots spectrograms using manually specified observation IDs
    """
    os.makedirs(save_folder, exist_ok=True)

    save_name = 'spec_obs_' + '_'.join(observations)
    save_path = os.path.join(save_folder, save_name)

    plot_flare(save_path=save_path, obs_ids=observations)


def plot_by_flarelist(save_folder, flare_csv, flare_range=None):
    """
    plots spectrograms and light curves using flare metadata
    """
    os.makedirs(save_folder, exist_ok=True)
    flare_data = get_flarelist(flare_csv)

    for i, flare_row in flare_data.iterrows():
        if flare_range and not (flare_range[0] <= i < flare_range[1]):
            continue

        save_path = os.path.join(save_folder, f"{i+2}_flareID_{flare_row['flare_id']}")

        try:
            logging.info(f"Processing flare {i+2} with ID {flare_row['flare_id']}")
            
            if os.path.exists(f"{save_path}.png"):
                logging.info(f"Output file already exists. Skipping...")
                continue

            should_stop = plot_flare(save_path=save_path, row=flare_row)
            if should_stop:
                logging.info(f"Continuing...")
                continue

        except Exception as e:
            logging.error(f"{e} \n{traceback.format_exc()}")

        finally:
            plt.close('all')
            gc.collect()
            logging.info(f"***************************************")


if __name__ == "__main__":
    main()
