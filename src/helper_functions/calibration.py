import subprocess, os, shutil, logging
from pathlib import Path
import numpy as np
from casacore.tables import table

log = logging.getLogger(__name__)
from helper_functions.utils import get_observation_path, get_metafits_files


def write_point_srclist(ms_path: Path, flux_jy: float, out_yaml: Path):
    """write a one-line yaml sky model at the ms phase centre"""
    fld = table(str(ms_path / "FIELD"))
    ra_rad, dec_rad = fld.getcol("PHASE_DIR")[0, 0, :]
    fld.close()
    ra_deg, dec_deg = np.degrees([ra_rad, dec_rad])
    out_yaml.write_text(f"""
calibrator:
  - ra: {ra_deg}
    dec: {dec_deg}
    comp_type: point
    flux_type:
      list:
        - freq: 1.54e8
          i: {flux_jy}
""")
    log.info("wrote sky model → %s", out_yaml)


def run_di_calibrate(cal_ms: Path, flux_jy: float, sol_path: Path, work_root):
    """derive direction-independent gains"""
    yaml_path = sol_path.with_suffix(".yaml")
    write_point_srclist(cal_ms, flux_jy, yaml_path)

    os.environ.setdefault(
        "MWA_BEAM_FILE",
        str(Path.home() / "local/share/mwa_full_embedded_element_pattern.h5")
    )

    subprocess.run(
        ["hyperdrive", "di-calibrate",
         "-d", str(cal_ms),
         "-s", str(yaml_path),
         "-o", str(sol_path)],
        check=True
    )

def apply_solutions(raw_ms: Path, sol_path: Path, work_root) -> Path:
    """apply gains and return new *_cal.ms path"""
    out_ms = raw_ms.with_name(f"{raw_ms.stem}_cal{raw_ms.suffix}")
    if out_ms.exists():
        shutil.rmtree(out_ms)
    subprocess.run(
        ["hyperdrive", "solutions-apply",
         "-d", str(raw_ms),
         "-s", str(sol_path),
         "-o", str(out_ms)],
        check=True
    )
    return out_ms
