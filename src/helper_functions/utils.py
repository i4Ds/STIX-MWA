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
    candidates = [p for p in root.glob(f"*{observation_id}*.tar") if p.is_file()]
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


def find_data_column(ms_path: Path) -> str:
    """return corrected_data if it exists, else data"""
    with table(str(ms_path)) as t:
        cols = t.colnames()
    return "CORRECTED_DATA" if "CORRECTED_DATA" in cols else "DATA"


def reset_dir(path: Path):
    """ensure directory exists and is empty"""
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def get_time_info(ms_path: Path):
    """return (start_time, interval_size, interval_count)"""
    obs = table(str(ms_path))
    obs.unlock()
    centers = obs.getcol("TIME")
    sizes   = obs.getcol("INTERVAL")
    radius  = sizes / 2.0
    mjd0    = np.min(centers - radius) / 86400.0
    mjd1    = np.max(centers + radius) / 86400.0
    dt      = TimeDelta(sizes[0], format="sec")
    count   = int((mjd1 - mjd0) * 86400 / dt.sec)
    return Time(mjd0, format="mjd", scale="utc"), dt, count


def ms_central_frequency(ms_path: Path) -> float:
    """
    read the mean channel frequency [hz] from the spectral_window table
    """
    from casacore.tables import table
    spw = table(f"{ms_path}::SPECTRAL_WINDOW")
    freqs = spw.getcol("CHAN_FREQ")  # shape: (nspw, nchan) or (nchan,)
    spw.close()
    f = np.mean(freqs)
    return float(f)


def _extract_metafits_from_tar(metafits_root: str, obs_id: str) -> Path:
    """
    find and extract the .metafits that matches obs_id from a *_vis_meta.tar
    the search is recursive under metafits_root and prefers tar files whose
    name contains the obs_id.
    """
    root = Path(metafits_root)
    # search for matching vis_meta tars
    patt1 = list(root.rglob(f"{obs_id}_*_vis_meta.tar"))
    patt2 = list(root.rglob(f"*{obs_id}*vis_meta.tar"))
    tars = sorted(set(patt1 + patt2))
    if not tars:
        raise FileNotFoundError(f"no vis_meta tar found for obs_id {obs_id} under {metafits_root}")

    tar_path = tars[0]
    out_dir = TMP_DIR / f"{obs_id}_metafits"
    out_dir.mkdir(parents=True, exist_ok=True)

    with tarfile.open(tar_path, "r") as tf:
        # prefer a .metafits file whose filename contains the obs_id
        members = [m for m in tf.getmembers() if m.name.lower().endswith(".metafits")]
        if not members:
            raise FileNotFoundError(f"no .metafits inside {tar_path}")
        mf = next((m for m in members if obs_id in Path(m.name).name), members[0])

        tf.extract(mf, path=out_dir)
        extracted = out_dir / Path(mf.name).name
        if not extracted.exists():
            raise FileNotFoundError(f"failed to extract to {extracted}")

    logging.info("extracted metafits (%s) → %s", tar_path.name, extracted)
    return extracted
