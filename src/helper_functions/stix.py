import numpy as np
import pandas as pd
from stixdcpy import auxiliary as aux
from stixdcpy.quicklook import LightCurves
from helper_functions.utils import set_x_ticks
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


def get_flarelist(path_to_flarelist):
    try:
        return pd.read_csv(path_to_flarelist)
    except FileNotFoundError:
        logging.info(f"Flarelist file not found at {path_to_flarelist}.")


def load_stix_light_curve(start_utc, end_utc):
    try:
        return LightCurves.from_sdc(start_utc, end_utc, ltc=True)
    except Exception as e:
        logging.info(f"Error loading light curves: {e}")


def get_position(start, end):
    try:
        return aux.Ephemeris.from_sdc(start_utc=start, end_utc=end, steps=1)
    except Exception as e:
        logging.info(f"Error loading position data: {e}")


### plotting functions ###

def plot_stix_light_curve(row, ax, energy_range):
    """
    plots STIX light curves for the flare defined in row.
    """
    start_utc = row['stix_start_UTC']
    end_utc = row['stix_end_UTC']

    light_curve = load_stix_light_curve(start_utc, end_utc)

    if not light_curve.data and not light_curve:
        ax.text(0.5, 0.5, 'LC not available!', ha='center', va='center')
        return ax

    for i in range(5) if energy_range is None else range(energy_range[0], energy_range[1] + 1):
        ax.plot(
            light_curve.time,
            np.asarray(light_curve.counts)[i, :],
            label=light_curve.energy_bins['names'][i]
        )
    ax.set_ylabel('Counts')
    set_x_ticks(ax)
    ax.set_yscale('log')
    ax.legend(loc='upper right')
    ax.set_title(f"STIX Light curves for the flare with ID {row['flare_id']}")
    ax.set_xlim(pd.to_datetime(start_utc), pd.to_datetime(end_utc))
    ax.set_xlabel("Time (UTC)")
    return ax