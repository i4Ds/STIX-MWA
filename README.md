# STIX–MWA Data Analysis

This repository provides tools for analyzing and comparing data from 
[STIX](https://datacenter.stix.i4ds.net/) (Spectrometer/Telescope for Imaging X-rays aboard Solar Orbiter) 
and the [MWA](https://www.mwatelescope.org/) (Murchison Widefield Array).

---

## Table of Contents
- [Overview](#overview)
- [Environment Setup](#environment-setup)
- [Repository Layout](#repository-layout)
- [Data](#data)
- [Paper](#paper)
- [Known Issues / TODO](#known-issues--todo)

---

## Overview

This project integrates STIX and MWA observations to:
- identify overlapping solar flare events,
- download and preprocess MWA visibilities,
- generate dynamic spectra and light curves,
- compare flare timing and locations across instruments.

It also includes optional support for e-CALLISTO data and imaging routines using WSClean/CASA.

---

## Environment Setup

This codebase supports **Python 3.8–3.10** to ensure compatibility with recent versions of CASA.

## Environment Setup

### 1. Conda Environment
Ensure you have Conda or a compatible tool installed. Then:
```bash
conda env create -f environment.yml -n stixmwa
conda activate stixmwa
```

### 2. Additional Dependencies
Some required libraries must be installed manually.

#### Manta-ray
```bash
git clone https://github.com/ICRAR/manta-ray-client.git
cd manta-ray-client
pip install -e .
```

Then replace the api.py in manta-ray-client/mantaray/api with api.py from src/helper_functions.

#### casacore
```bash
git clone https://github.com/aaijmers/wsclean.git
cd wsclean
mkdir build && cd build
cmake .. \
  -DCMAKE_INSTALL_PREFIX=$HOME/casacore-install \
  -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
make install
```

#### WSClean
```bash
# Navigate to the WSClean source directory (adjust path as needed)
cd ~/STIX-MWA/src/wsclean

# Patch CMakeLists: change C++20 to C++17
sed -i 's/set(CMAKE_CXX_STANDARD *20)/set(CMAKE_CXX_STANDARD 17)/' CMakeLists.txt
sed -i 's/set(CMAKE_CXX_STANDARD *20)/set(CMAKE_CXX_STANDARD 17)/' external/aocommon/CMakeLists.txt

# Clean and configure build
rm -rf build
mkdir build && cd build
cmake .. \
  -DWSCLEAN_WITH_CHGCENTRE=OFF \
  -DWSCLEAN_WITH_PYTHON=OFF \
  -DWSCLEAN_WITH_RADLER=OFF \
  -DBUILD_TESTING=OFF \
  -DCMAKE_CXX_FLAGS="-march=x86-64 -mtune=generic"
cmake --build . -j$(nproc)
```

After sucessful casacore instalation, install casatools and casatasks with pip:
```bash
pip install --index-url https://casa-pip.nrao.edu/repository/pypi-casa-release/simple casatools
pip install --index-url https://casa-pip.nrao.edu/repository/pypi-casa-release/simple casatasks
```

Then export the following paths (update to your install locations):
```bash
export PATH="$HOME/STIX-MWA/wsclean/build:$PATH"
export LD_LIBRARY_PATH="$HOME/casacore-install/lib:$LD_LIBRARY_PATH"
export PKG_CONFIG_PATH="$HOME/casacore-install/lib/pkgconfig:$PKG_CONFIG_PATH"
```

### 3. Environment Variables
A sample `.env.example` file is provided. Copy it to `.env` and fill in required values (e.g., API keys, data paths).

---

## Repository Layout

- `src/find_flares_in_mwa.py` – queries STIX flare list and checks overlap with MWA obs times.
- `src/get_mwa_data.py` – downloads raw data from MWA ASVO.
- `src/compare_mwa_stix_locations.py` – cross-comparison of event positions.
- `src/plot.py` – generates light curves and spectrogram plots.
- `src/run_wsclean.py` – runs WSClean imaging pipeline.

### helper functions (`src/helper_functions/`)
- `stix.py` – STIX flare parsing and lightcurve handling.
- `mwa_asvo.py` – querying MWA ASVO API.
- `spectrogram.py` – building spectrograms from visibilities.
- `ecallisto.py` – optional integration of e-CALLISTO data.
- `plot_flare.py` – visualization helpers.
- `utils.py` – shared helpers.
- `calibration.py`, `selfcal.py`, `mwa_imaging.py` – calibration and imaging routines.

---

## Data

All observations are already downloaded and are located on the /mnt/nas05/data02/predrag/data/mwa_data

---

## Paper

Start of the paper containing introduction, detailed related-work and methodology is located in the _results folder.

---

## Known Issues / TODO
- CASA compatibility can be version-sensitive.
- Spectrogram generation is slow for full observations → parallelization would help.
- Some helper modules (`calibration.py`, `selfcal.py`) are experimental.
