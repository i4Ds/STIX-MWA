import os
import rootutils
from pathlib import Path
from dateutil import parser
from dotenv import load_dotenv
from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
import tarfile
import tempfile
import re
import shutil, time, stat
import logging

 # setup project root and environment variables immediately
rootutils.setup_root(Path(__file__).resolve(), indicator=".project-root", pythonpath=True)
load_dotenv()

 # define root path to data
ROOT_PATH_TO_DATA = Path(os.getenv("ROOT_PATH_TO_DATA", None))


def get_root_path_to_data():
    if ROOT_PATH_TO_DATA is None:
        raise ValueError("ROOT_PATH_TO_DATA not set.")
    return ROOT_PATH_TO_DATA


def safe_parse_time(t):
    return parser.parse(t).replace(microsecond=0) if isinstance(t, str) else t.replace(microsecond=0)


def set_x_ticks(ax):
    locator = AutoDateLocator(minticks=3, maxticks=7)
    formatter = ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)


def get_observation_path(observation_id):
    root = get_root_path_to_data()
    candidates = [p for p in root.glob(f"{observation_id}*.tar") if p.is_file()]
    if not candidates:
        raise FileNotFoundError(f"no archive found for {observation_id} in {root}")
    return candidates[0]


def get_ms_files(tar_path, scratch_root):
    """extract a *.ms from *tar_path* and return (ms_path, tmp_dir)"""

    with tarfile.open(tar_path, "r") as tar:
        # find the first directory that ends with '.ms'
        ms_names = sorted(
            [m for m in tar.getmembers()
            if m.isdir() and m.name.endswith(".ms")],
            key=lambda s: int(s.name.split("ch")[1].split("-")[0]),   # grab first channel as int
        )
        if not ms_names:
            raise ValueError("no .ms directory found in archive")

        logging.info(ms_names)
        
        ms_dirinfo = ms_names[0]
        logging.info(f"extracting {ms_dirinfo.name}")

        # collect that directory plus every member under it
        prefix = ms_dirinfo.name.rstrip("/") + "/"
        members = [m for m in tar.getmembers()
                   if m.name == ms_dirinfo.name or m.name.startswith(prefix)]
                   
        # create a temporary workspace on scratch_root
        tmp_dir = tempfile.mkdtemp(dir=scratch_root)
        tar.extractall(path=tmp_dir, members=members)

    return Path(tmp_dir) / ms_dirinfo.name.lstrip("./")



def get_metafits_files(tar_path, scratch_root):
    """extract a *.metafits from *tar_path* and return (ms_path, tmp_dir)"""

    with tarfile.open(tar_path, "r") as tar:
        # find the first directory that ends with '.ms'
        ms_names = [m for m in tar.getmembers() if m.name.endswith(".metafits")]
        if not ms_names:
            raise ValueError("no .metafits directory found in archive")

        logging.info(ms_names)
        
        ms_dirinfo = ms_names[0]
        logging.info(f"extracting {ms_dirinfo.name}")

        # collect that directory plus every member under it
        prefix = ms_dirinfo.name.rstrip("/") + "/"
        members = [m for m in tar.getmembers()
                   if m.name == ms_dirinfo.name or m.name.startswith(prefix)]
                   
        # create a temporary workspace on scratch_root
        tmp_dir = tempfile.mkdtemp(dir=scratch_root)
        tar.extractall(path=tmp_dir, members=members)

    return Path(tmp_dir) / ms_dirinfo.name.lstrip("./")
