import os
import logging
import requests
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from astropy.io import fits
from datetime import timedelta
from sunpy.time import parse_time
import matplotlib.pyplot as plt
from helper_functions.utils import set_x_ticks, safe_parse_time


class CallistoSpectrogram:
    """
    basic class for reading and plotting e-callisto dynamic spectra
    """

    def __init__(self, filepath):
        """
        initialize by reading a .fit.gz file
        """
        self.filepath = filepath
        self.hdul = fits.open(filepath)

        raw_data = self.hdul[0].data
        raw_data = np.ma.filled(raw_data, fill_value=np.nan)
        self.data = np.asarray(raw_data, dtype=float)

        self.header = self.hdul[0].header

        self.n_time = self.header['NAXIS1']
        self.n_freq = self.header['NAXIS2']

        self.freq_axis = np.linspace(
            self.header['CRVAL2'],
            self.header['CRVAL2'] + self.header['CDELT2'] * (self.n_freq - 1),
            self.n_freq
        )

        self.freq_axis = self.freq_axis[::-1]
        self.data = self.data[::-1, :]

        time_start = self.header['DATE-OBS'] + 'T' + self.header['TIME-OBS']

        self.start_time = parse_time(time_start)
        self.time_axis = [
            self.start_time + timedelta(seconds=i * self.header['CDELT1'])
            for i in range(self.n_time)
        ]
        self.end_time = self.time_axis[-1]


def get_ecallisto_data(flare_start, flare_end):
    """
    find multiple callisto files and combine their data into one stacked spectrogram
    assumes all files have the same frequency axis
    """

    ecallisto_paths = find_matching_callisto_files(flare_start, flare_end)

    all_data = []
    all_time = []
    freq_axis = None

    for path in sorted(ecallisto_paths):  # sort by time
        logging.info(f"Loading {path}")
        try:
            spec = CallistoSpectrogram(path)

            # compare frequency axes
            if freq_axis is None:
                freq_axis = spec.freq_axis
            elif not np.allclose(freq_axis, spec.freq_axis, atol=0.01):
                logging.info(f"Skipping {path}: incompatible frequency axis")
                continue
            
            all_data.append(spec.data)
            all_time.extend(spec.time_axis)

        except Exception as e:
            logging.info(f"Failed to load {path}: {e}")
            continue

    if not all_data:
        return None, None, None

    combined_data = np.concatenate(all_data, axis=0)  # stack along time axis
    return combined_data, all_time, freq_axis


def find_matching_callisto_files(flare_start, flare_end, download_folder='/mnt/nas05/data02/predrag/data/ecallisto'):
    """
    find and download the first matching e-callisto file whose observation overlaps with the given time range
    tries multiple stations if none found
    """

    date = flare_start.date()
    os.makedirs(download_folder, exist_ok=True)

    files, base_url = list_callisto_files(date)
    if not files:
        return None
    
    matching_files = []

    for fname in files:
        if "australia-assa" in fname.lower():
            obs_guess, type = parse_file_time(fname)
            if type == "62.fit.gz":
                file_duration = 15  
                obs_start = obs_guess
                obs_end = obs_start + timedelta(minutes=file_duration)

                # only keep files that fully contain the flare
                if normalize(obs_start) <= normalize(flare_end) and normalize(obs_end) >= normalize(flare_start):

                    file_url = base_url + fname
                    local_path = os.path.join(download_folder, fname)
                    local_file = download_callisto_file(file_url, local_path)

                    if local_file is None:
                        continue

                    matching_files.append(local_file)
    return matching_files


def list_callisto_files(date):
    """
    list available callisto files for a given date and station
    """
    base_url = 'http://soleil.i4ds.ch/solarradio/data/2002-20yy_Callisto'
    url = f"{base_url}/{date.year}/{date.strftime('%m')}/{date.strftime('%d')}/"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.info(f"could not connect to {url}: {e}")
        return [], url

    soup = BeautifulSoup(response.text, 'html.parser')
    files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.fit.gz')]
    return files, url


def download_callisto_file(file_url, save_path):
    """
    download a callisto .fit.gz file and save it locally if not already downloaded
    """
    if os.path.exists(save_path):
        logging.info(f"file already downloaded: {save_path}")
        return save_path

    try:
        r = requests.get(file_url, stream=True)
        r.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        logging.info(f"downloaded file: {save_path}")
        return save_path
    except Exception as e:
        logging.info(f"failed to download {file_url}: {e}")
        return None


def parse_file_time(filename):
    """
    parse utc time from filename like 'STATION_YYYYMMDD_HHMMSS_59.fit.gz'
    """
    try:
        parts = filename.split('_')
        if len(parts) < 3:
            return None
        date_part = parts[1]
        time_part = parts[2]
        type_part = parts[3]
        return datetime.strptime(date_part + time_part, '%Y%m%d%H%M%S'), type_part
    except Exception:
        return None


def normalize(ts):
    return ts.replace(tzinfo=None)


def plot_ecallistio(row, ax, fig, gs):
    """ 
    plots e-Callisto spectrogram for Australia-ASSA
    """
    flare_start = safe_parse_time(row['mwa_start_UTC'])
    flare_end = safe_parse_time(row['mwa_end_UTC'])

    data, time_axis, freq_axis = get_ecallisto_data(flare_start, flare_end)
    if data is not None:
        im = ax.imshow(
            data,
            aspect='auto',
            origin='lower',
            extent=[time_axis[0].to_datetime(), time_axis[-1].to_datetime(), freq_axis[0], freq_axis[-1]],
        )
        set_x_ticks(ax)
        ax.set_title("e-Callisto spectrogram for Australia-ASSA")
        ax.set_xlabel('Time [UTC]')
        ax.set_ylabel('Frequency [MHz]')
        ax.set_xlim(safe_parse_time(flare_start), safe_parse_time(flare_end))
        cbar_ax = fig.add_subplot(gs[4, 1])
        plt.colorbar(im, cax=cbar_ax)
    else:
        ax.text(0.5, 0.5, 'No matching e-CALLISTO files found', ha='center', va='center')