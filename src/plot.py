import os
import gc
import logging 
import traceback
import matplotlib.pyplot as plt
from helper_functions.stix import get_flarelist
from helper_functions.plot_flare import plot_flare


def main():
    """
    plots data based on provided observation ids or STIX flares matched with mwa metadata.
    """
    observations = ['1126847624']  # set to [] to use flare list
    observations = []

    if observations:
        save_folder = '../results/plots/spectrograms'
        plot_by_observations(observations, save_folder)
    else:
        plot_by_flarelist(flare_range=None)  # or flare_range=(0, 3000)


def plot_by_observations(observations, save_folder):
    """
    plots spectrograms using manually specified observation IDs
    """
    os.makedirs(save_folder, exist_ok=True)

    save_name = 'spec_obs_' + '_'.join(observations)
    save_path = os.path.join(save_folder, save_name)

    plot_flare(save_path=save_path, obs_ids=observations)


def plot_by_flarelist(flare_range=None):
    """
    plots spectrograms and light curves using flare metadata
    """
    for use_time_corrected in [True, False]:
        save_folder = (
            '../results/plots/new_spectrograms_and_light_curves/time_corrected'
            if use_time_corrected else
            '../results/plots/new_spectrograms_and_light_curves/time_not_corrected'
        )
        os.makedirs(save_folder, exist_ok=True)

        flare_file = (
            "../files/flares_recorded_by_mwa_with_time_correction.csv"
            if use_time_corrected else
            "../files/flares_recorded_by_mwa_no_time_correction.csv"
        )
        flare_data = get_flarelist(flare_file)

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
