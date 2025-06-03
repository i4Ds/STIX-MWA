# STIX MWA Data Analysis

This repository contains code to analyse and compare [STIX](https://datacenter.stix.i4ds.net/)
with [MWA](https://www.mwatelescope.org/) data.

## Getting started

Make sure you have a working [WSClean installation](https://wsclean.readthedocs.io/en/latest/installation.html).
Ensure you have Conda or a compatible tool
installed ([Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html) recommended).

First, create the environment from the lock file:

```sh
micromamba create --file conda-lock.yml --name stix-mwa
```

Then, activate it:

```sh
micromamba activate stix-mwa
```

Finally, run the desired Python script or start Jupyter Lab:

```sh
jupyter lab
```

> **Micromamba & PyCharm** <br>
> PyCharm currently [does not support Mamba](https://youtrack.jetbrains.com/issue/PY-58703/Setting-interpreter-to-mamba-causes-PyCharm-to-stop-accepting-run-configurations).
> As a workaround, install and use Conda in PyCharm but point it to the same environment:
>
> ```sh
> micromamba install conda-forge::conda
> ```
>
> PyCharm will be happy and you can keep using any other tool to manage the environment.

## Various commands

Update the lock file (from `environment.yml`):

```bash
conda-lock --micromamba
```
