# STIX–MWA Data Analysis

This repository provides tools for analyzing and comparing data from [STIX](https://datacenter.stix.i4ds.net/) (Spectrometer/Telescope for Imaging X-rays aboard Solar Orbiter) and the [MWA](https://www.mwatelescope.org/) (Murchison Widefield Array).

## Table of Contents
- [Getting Started](#getting-started)
- [Environment Setup](#environment-setup)
  - [1. Conda Environment](#1-conda-environment)
  - [2. Additional Dependencies](#2-additional-dependencies)
    - [Manta-ray](#manta-ray)
    - [casacore](#casacore)
    - [WSClean](#wsclean)
- [Environment Variables](#environment-variables)
- [Contact](#contact)

## Getting Started
This codebase supports **Python 3.8–3.10** to ensure compatibility with recent versions of CASA.

## Environment Setup

### 1. Conda Environment
Ensure you have Conda or a compatible tool installed. Then:
```bash
conda env create -f environment.yaml -n stixmwa
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

## Environment Variables
Create a \`.env\` file in the project root directory. Use the provided \`.env.example\` file as a template.  

## Contact
For questions, suggestions, or contributions, please [open an issue](https://github.com/i4Ds/STIX-MWA/issues) or submit a pull request.
