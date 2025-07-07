import os
import rootutils
from pathlib import Path
from dateutil import parser
from dotenv import load_dotenv
from matplotlib.dates import AutoDateLocator, ConciseDateFormatter


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