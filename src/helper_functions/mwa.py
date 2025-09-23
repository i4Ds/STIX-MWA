import ast
import pyvo
import logging
import requests
import numpy as np
import pandas as pd
from dateutil import parser
from datetime import timedelta
from collections import Counter
import matplotlib.pyplot as plt
from helper_functions.spectrogram import get_spectrogram
from helper_functions.utils import safe_parse_time, set_x_ticks


def get_mwa_metadata(start_time=None, end_time=None, obs_ids=None):
    """
    get metadata for an mwa observation
    """
    # construct the ADQL query
    if obs_ids is not None:
        ids_formatted = ', '.join(f"'{id}'" for id in obs_ids)
        query = f"SELECT * FROM mwa.observation WHERE obs_id IN ({ids_formatted})"
    elif start_time is not None and end_time is not None:
        query = f"""
        SELECT * FROM mwa.observation
        WHERE stoptime_utc >= '{format_time_for_mwa(start_time)}'
        AND starttime_utc <= '{format_time_for_mwa(end_time)}'
        """
    else:
        raise ValueError("invalid parameters. provide either 'obs_id' or both 'start_time' and 'end_time'.")

    # tap endpoint
    tap_url = "https://vo.mwatelescope.org/mwa_asvo/tap/sync"

    # form data required for a TAP sync query
    data = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "csv",
        "QUERY": query
    }

    # make the request
    response = requests.post(tap_url, data=data)
    response.raise_for_status()  # raise error if HTTP status is not 200

    # convert to pandas dataframe
    from io import StringIO
    df = pd.read_csv(StringIO(response.text))
    df = df.sort_values(by="starttime_utc").reset_index(drop=True)
    logging.info(f"number of found observations is {len(df)}")
    return df


def format_time_for_mwa(time_str):
    """
    format time string for mwa queries
    """
    dt = parser.parse(time_str)
     # format the datetime to the desired format, cutting off milliseconds to 3 digits
    formatted_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return formatted_time


def get_mwa_light_curve(spectrogram):
    """
    download and process mwa light curve
    """
    return np.ma.sum(spectrogram, axis=0) if spectrogram is not None else None


def estimate_time_resolution(spectrograms, times):
    """
    estimates time resolution in seconds from spectrogram shapes and time ranges
    """
    resolutions = []
    for dspec, (start, end) in zip(spectrograms, times):
        spec = dspec['spec']
        start = safe_parse_time(start)
        end = safe_parse_time(end)

        duration = (end - start).total_seconds()
        if spec.shape[1] > 0:
            res = duration / spec.shape[1]
            resolutions.append(res)

    if resolutions:
        return round(np.mean(resolutions))
    return None


### plotting functions ###

def plot_mwa_from_obs_ids(obs_ids, axes, gs, fig, data_path):
    """ 
    plots MWA spectrograms for observation IDs defined in obs_ids.
    """
    axes[0].text(0.5, 0.5, 'LC not available!', ha='center', va='center')
    axes[0].axis('off')

    mwa_metadata = get_mwa_metadata(obs_ids=obs_ids)
    spec, times, freqs = get_spectrogram(mwa_metadata, data_path)

    if spec is None:
        axes[1].text(0.5, 0.5, 'MWA spectrogram not available!', ha='center', va='center')
        return None, None

    im, time_axis = draw_mwa_spectrogram(spec, times, freqs, axes[1], safe_parse_time(times[0][0]), safe_parse_time(times[-1][-1]))
    cbar_ax = fig.add_subplot(gs[1, 1])
    plt.colorbar(im, cax=cbar_ax, label='Power')
    axes[1].set_title('Dynamic spectrum from MWA observations')

    return spec, time_axis


def plot_mwa_from_flare_row(flare_row, ax, fig, gs, path_to_data):
    """ 
    plots MWA spectrograms for the flare defined in flare_row.
    """
    start_time = flare_row["start_UTC"]
    end_time = flare_row["end_UTC"]

    mwa_metadata = get_mwa_metadata(start_time=start_time, end_time=end_time)
    if mwa_metadata.empty:
        ax.text(0.5, 0.5, 'MWA metadata not available!', ha='center', va='center')
        return ax, []
    
    spec, times, freqs = get_spectrogram(mwa_metadata, path_to_data)
    if spec is None or not times or not times[0]:
        return ax, []
    
    im, time_axis = draw_mwa_spectrogram(spec, times, freqs, ax, start_time, end_time)

    cbar_ax = fig.add_subplot(gs[1, 1])
    plt.colorbar(im, cax=cbar_ax)

    project_summary = get_project_summary(flare_row["projectids"])
    ax.set_title(f'Dynamic spectrum from MWA observations; Project IDs: {project_summary}')
    return spec, time_axis


def draw_mwa_spectrogram(spec, times, freqs, ax, start_cut, end_cut, time_res=4):
    """
    draws mwa spectrogram using fixed 4s resolution and time-aligned x-axis, including gaps
    """
    start_time = safe_parse_time(times[0][0])
    end_time = safe_parse_time(times[-1][-1])
    num_cols = spec.shape[1]

     # generate equally spaced time axis between start_time and end_time
    dt = (end_time - start_time) / (num_cols - 1) if num_cols > 1 else timedelta(seconds=time_res)
    time_axis = [start_time + i * dt for i in range(num_cols)]

    im = ax.imshow(
        spec,
        aspect='auto',
        origin='lower',
        extent=[time_axis[0], time_axis[-1], freqs[0], freqs[-1]]
    )

    ax.set_ylabel('Frequency [MHz]')
    ax.set_xlabel('Time (UTC)')
     # restrict yticks to valid frequency range
    ax.set_yticks([yt for yt in ax.get_yticks() if freqs[0] <= yt <= freqs[-1]])
    set_x_ticks(ax)
     # set xlim to the start and end time of the flare
    ax.set_xlim(safe_parse_time(start_cut), safe_parse_time(end_cut))
    return im, time_axis


def get_project_summary(projectids):
    """
    get project summary from the metadata
    """
     # get the project ids from the metadata
    project_ids = ast.literal_eval(projectids)
     # count unique project ids
    project_counter = Counter(project_ids)
     # nicely format the result
    return ', '.join(f'{pid} ({count})' for pid, count in project_counter.items())


def plot_mwa_light_curve(spec, time_axis, ax, flare_row):
    """
    plots the integrated mwa light curve using a shared time axis
    """    
    light_curve = get_mwa_light_curve(spec)

    if light_curve is None:
        ax.text(0.5, 0.5, 'MWA light curve not available!', ha='center', va='center')
        return

    ax.plot(time_axis, light_curve)
    set_x_ticks(ax)

     # set xlim to the start and end time of the flare
    if flare_row is None:
        ax.set_xlim(time_axis[0], time_axis[-1])
    else:
        ax.set_xlim(safe_parse_time(flare_row["start_UTC"]), safe_parse_time(flare_row["end_UTC"]))
    ax.set_title("Integrated MWA light curve")
    ax.set_ylabel("Total intensity (log scale)")
    ax.set_xlabel("Time (UTC)")