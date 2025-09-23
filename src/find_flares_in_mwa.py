"""
matches STIX flares with MWA observations based on time overlap during local daylight
STIX flare data from https://github.com/hayesla/stix_flarelist_science
"""
import os
import pandas as pd
import pytz
import pyvo
from datetime import timedelta, time as dt_time
from astral import LocationInfo
from astral.sun import sun
from sunpy.coordinates.sun import earth_distance
from sunpy.time import parse_time
import astropy.units as u
from astropy.constants import c
import logging
import numpy as np


def main():
    """
    runs the main workflow to match stix flares with mwa observations
    """

    stix_flares_path = "../files/STIX_flarelist_w_locations_20210214_20250228_version1_python.csv"

    # load mwa metadata and stix data
    mwa_data = load_and_preprocess_mwa_metadata()
    mwa_location = LocationInfo("MWA", "Australia", "Australia/Perth", latitude=-26.7033, longitude=116.6708)

    # load and preprocess stix flare data
    stix_data = load_and_preprocess_stix_data(stix_flares_path)

    # analyse, filter and enrich flares
    flares_df, num_samples = analyze_and_filter_flares(stix_data, mwa_data, mwa_location)
    flares_df = attach_mwa_and_calibrator_info(flares_df, mwa_data)

    # decide output file name
    save_path = '../files/flares_recorded_by_mwa_test.csv'

    flares_df.to_csv(save_path, index=False)
    print_summary(num_samples)


def load_and_preprocess_mwa_metadata(save: bool = False, path_to_save: str = "../files") -> pd.DataFrame:
    """
    loads mwa metadata via tap and pre‑processes datetime fields and calibration flags
    """
    tap_service = pyvo.dal.TAPService("http://vo.mwatelescope.org/mwa_asvo/tap")

    query = """
        SELECT *
        FROM mwa.observation
        WHERE starttime_utc > '2021-02-14T00:00:00Z'
          AND projectid = 'G0002'
    """

    result = tap_service.search(query)
    mwa = result.to_table().to_pandas().sort_values(by='starttime_utc').reset_index(drop=True)

    # make sure datetime columns are timezone aware (utc)
    for col in ['starttime_utc', 'stoptime_utc']:
        mwa[col] = pd.to_datetime(mwa[col]).apply(
            lambda x: x.tz_convert('UTC') if x.tzinfo else x.tz_localize('UTC')
        )

    # normalise calibration column to boolean
    if 'calibration' in mwa.columns:
        mwa['calibration'] = mwa['calibration'].astype(str).str.lower().isin(['true', '1', 'yes'])
    else:
        mwa['calibration'] = False

    # guarantee obsname exists and is a string
    if 'obsname' not in mwa.columns:
        mwa['obsname'] = ''
    else:
        mwa['obsname'] = mwa['obsname'].fillna('')

    if save:
        mwa.to_csv(os.path.join(path_to_save, "mwa_metadata.csv"), index=False)

    return mwa


def load_and_preprocess_stix_data(filepath: str) -> pd.DataFrame:
    """
    loads stix flare data from csv and optionally applies light travel time correction
    """

    stix = pd.read_csv(filepath)
    stix['start_utc'] = pd.to_datetime(stix['start_UTC']).dt.tz_localize('UTC')
    stix['end_utc'] = pd.to_datetime(stix['end_UTC']).dt.tz_localize('UTC')
    stix = stix[stix['visible_from_earth']].reset_index(drop=True)
    stix['flare_duration_sec'] = (stix['end_utc'] - stix['start_utc']).dt.total_seconds()

    return stix


def analyze_and_filter_flares(stix_data: pd.DataFrame, mwa_data: pd.DataFrame, mwa_location: LocationInfo):
    """
    computes flare–mwa overlaps, converts to dataframe and basic filtering + sorting
    """

    flares_df, num_samples = analyze_flare_data(stix_data, mwa_data, mwa_location)
    flares_df = find_flares_with_overlap(flares_df, overlap_percentage=1)
    flares_df = sort(flares_df, sort_values_list=['overlap_percentage', 'goes_numeric'])

    # keep core columns (additional columns added later)
    flares_df = flares_df[[
        'flare_id', 'GOES_class', 'start_UTC', 'end_UTC', 'flare_duration_sec', 'overlap_percentage'
    ]].reset_index(drop=True)

    return flares_df, num_samples


def analyze_flare_data(stix: pd.DataFrame, mwa_data: pd.DataFrame, mwa_location: LocationInfo):
    """
    loops through flares and calculates time overlap with mwa observations during daylight
    """

    time_overlap_data = []
    num_samples = {f'matching {i*10}-{(i+1)*10}%': 0 for i in range(10)}
    num_samples['num_of_matching_observations'] = 0

    for _, flare_row in stix.iterrows():
        flare_start, flare_end = flare_row['start_utc'], flare_row['end_utc']
        flare_duration = flare_row['flare_duration_sec']

        overlap = calculate_overlap(mwa_data, flare_start, flare_end, mwa_location)
        overlap_percentage = int(100 * overlap.total_seconds() / flare_duration)

        time_overlap_data.append({
            'flare_id': flare_row['flare_id'],
            'GOES_class': flare_row['GOES_class_time_of_flare'],
            'start_UTC': flare_row['start_utc'],
            'end_UTC': flare_row['end_utc'],
            'flare_duration_sec': int(flare_duration),
            'overlap_duration_sec': int(overlap.total_seconds()),
            'overlap_percentage': overlap_percentage
        })

        if overlap_percentage > 0:
            num_samples['num_of_matching_observations'] += 1
        for i in range(10):
            if i * 10 < overlap_percentage <= (i + 1) * 10:
                num_samples[f'matching {i*10}-{(i+1)*10}%'] += 1

    return pd.DataFrame(time_overlap_data), num_samples


def calculate_overlap(mwa_data: pd.DataFrame, flare_start, flare_end, mwa_location: LocationInfo):
    """
    returns total time overlap (timedelta) between flare and mwa observations during local daylight
    """
    mwa_relevant = mwa_data[
        (mwa_data['starttime_utc'] <= flare_end) &
        (mwa_data['stoptime_utc'] >= flare_start)
    ]

    if not mwa_relevant.empty:
        times = mwa_relevant[['starttime_utc', 'stoptime_utc']]
        print(f"{flare_start} - {flare_end} overlaps with {times} mwa observations")

    total_overlap = timedelta(seconds=0)

    for _, mwa_row in mwa_relevant.iterrows():
        overlap_start = max(flare_start, mwa_row['starttime_utc'])
        overlap_end = min(flare_end, mwa_row['stoptime_utc'])

        # daylight window for the given date in utc
        times = sun(mwa_location.observer, date=overlap_start.date(), tzinfo=pytz.UTC)
        sunrise, sunset = times['sunrise'], times['sunset']

        # handle wrap‑around for observations crossing midnight
        adjusted_sunrise = sunrise - timedelta(days=1) if dt_time(0, 0) <= overlap_start.time() < dt_time(12, 0) else sunrise
        adjusted_sunset = sunset + timedelta(days=1) if dt_time(17, 0) <= overlap_start.time() <= dt_time(23, 59) else sunset

        total_overlap_start = overlap_start
        total_overlap_end = overlap_end

        if total_overlap_start < total_overlap_end:
            total_overlap += total_overlap_end - total_overlap_start

    return total_overlap


def find_flares_with_overlap(df: pd.DataFrame, overlap_percentage: int):
    """
    filters dataframe for flares whose mwa overlap >= given percentage
    """
    return df[df['overlap_percentage'] >= overlap_percentage].reset_index(drop=True)


def sort(df: pd.DataFrame, sort_values_list=None):
    """
    sorts flares first by overlap percentage then by numeric goes class
    """
    if sort_values_list is None:
        sort_values_list = ['overlap_percentage', 'goes_numeric']

    df['goes_numeric'] = df['GOES_class'].apply(goes_class_to_numeric)
    return df.sort_values(by=sort_values_list, ascending=[False, False], ignore_index=True)


def goes_class_to_numeric(goes_class: str):
    """
    converts goes class (e.g. m5.6) to a numeric proxy for sorting
    """
    if pd.isna(goes_class):
        return -1

    scale = {'A': 1e-8, 'B': 1e-7, 'C': 1e-6, 'M': 1e-5, 'X': 1e-4}
    try:
        prefix = goes_class[0].upper()
        magnitude = float(goes_class[1:]) if len(goes_class) > 1 else 1.0
        return scale[prefix] * magnitude
    except (KeyError, ValueError):
        return -1


def attach_mwa_and_calibrator_info(flares_df: pd.DataFrame, mwa_data: pd.DataFrame) -> pd.DataFrame:
    """
    enriches each flare with matching mwa observation ids, names, and nearby calibrator info (<12 h)
    """
    # prepare empty list columns
    extra_cols = [
        'projectids',
        'obs_ids',
        'obs_names',
        'calibrator_obs_ids',
        'calibrator_obs_names',
        'calibrator_time_diff_hr'
    ]
    for col in extra_cols:
        flares_df[col] = [[] for _ in range(len(flares_df))]

    twelve_hours = timedelta(hours=12)

    for idx, row in flares_df.iterrows():
        flare_start = pd.to_datetime(row['start_UTC'])
        flare_end = pd.to_datetime(row['end_UTC'])

        # observations overlapping the flare
        matching_data = mwa_data[(mwa_data['starttime_utc'] <= flare_end) & (mwa_data['stoptime_utc'] >= flare_start)]
        if matching_data.empty:
            continue

        # basic observation information
        flares_df.at[idx, 'projectids'] = matching_data['projectid'].tolist()
        flares_df.at[idx, 'obs_ids'] = matching_data['obs_id'].tolist()
        flares_df.at[idx, 'obs_names'] = matching_data['obsname'].tolist()

        # pick the first observation as reference to locate calibrators
        reference_time = matching_data.iloc[0]['starttime_utc']

        # find calibrator observations within ±12 h of the reference
        calibrator_mask = (
            (mwa_data['calibration']) &
            (abs((mwa_data['starttime_utc'] - reference_time).dt.total_seconds()) <= twelve_hours.total_seconds())
        )
        calibrator_data = mwa_data[calibrator_mask]

        if not calibrator_data.empty:
            flares_df.at[idx, 'calibrator_obs_ids'] = calibrator_data['obs_id'].tolist()
            flares_df.at[idx, 'calibrator_obs_names'] = calibrator_data['obsname'].tolist()
            time_deltas = [round(abs((t - reference_time).total_seconds()) / 3600, 2) for t in calibrator_data['starttime_utc']]
            flares_df.at[idx, 'calibrator_time_diff_hr'] = time_deltas

    return flares_df


def print_summary(num_samples: dict):
    """
    prints a short summary of overlap statistics
    """
    for key, value in num_samples.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
