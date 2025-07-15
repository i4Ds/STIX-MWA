import os
import rootutils
from pathlib import Path
from dateutil import parser
from dotenv import load_dotenv
from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
import tarfile
import tempfile
import re

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
    candidates = list(root.glob(f"{observation_id}_*_ms.tar"))
    if not candidates:
        raise FileNotFoundError(f"no archive found for {observation_id} in {root}")
    return candidates[0]


def get_ms_files(fname):
    """
    extracts ms files from a tar archive and returns them sorted by channel number if available.
    """
    temp_dir = tempfile.mkdtemp()
    with tarfile.open(fname, 'r') as tar:
        ms_files = [member for member in tar.getmembers() if member.name.endswith('.ms')]
        if not ms_files:
            raise ValueError("No .ms files found in the tar archive.")
        else:
            tar.extractall(path=temp_dir)

     # sort by the channel number extracted from the filename
    def _extract_channel_or_fallback(name):
        """
        extracts channel number from filename or falls back to a large number to push it to the end
        """
        match = re.search(r'ch(\d+)(?:-|\.ms)', name)
        if match:
            return int(match.group(1))
        else:
            # fallback: extract the full number if filename is like '1355089520.ms'
            match = re.search(r'(\d+)', name)
            if match:
                return int(match.group(1))
            else:
                return float('inf')  # put files without any number last

    ms_files = sorted(ms_files, key=lambda f: _extract_channel_or_fallback(f.name))
    return ms_files, temp_dir