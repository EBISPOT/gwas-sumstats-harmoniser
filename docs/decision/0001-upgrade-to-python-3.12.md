# 1. Upgrade to Python 3.12

## Status

Accepted

## Context

The project was running on Python 3.9, which reached end-of-life in October 2025. The existing
`environments/requirements.txt` contained many pinned packages from 2020–2021 that no longer
received security patches and were incompatible with modern tooling.

Key pressures driving an upgrade:

- Python 3.9 is end-of-life; no further security or bug-fix releases.
- Several core dependencies (`pysam`, `duckdb`, `pyarrow`, `pandas`) had released major updates
  with significant performance and correctness improvements that required Python ≥ 3.10.
- `gwas-sumstats-tools`, a first-party dependency, was updated locally to support up to Python 3.12
  (`requires-python = ">=3.9,<3.13"`), unblocking the upgrade.
- Python 3.13 is not yet viable because `gwas-sumstats-tools` caps at `<3.13` and several
  compiled packages (e.g. `datrie`) do not yet publish wheels for 3.13.
- Python 3.11 was the highest version reachable without changes to `gwas-sumstats-tools`
  (PyPI 1.0.25 declares `<3.12`); Python 3.12 required using the locally updated version.

## Decision

Upgrade the project to **Python 3.12** and update all dependencies to their latest compatible
versions.

Changes made:

- `Dockerfile`: base image changed from `python:3.9-slim-bookworm` to `python:3.12-slim-bookworm`.
- `environments/requirements.txt`: fully regenerated from a clean Python 3.12 install.
  - Removed 12 stale transitive packages that are no longer part of the dependency tree
    (`appdirs`, `attrs`, `chardet`, `docutils`, `importlib-metadata`, `ipython-genutils`,
    `jupyter-core`, `nbformat`, `pyrsistent`, `traitlets`, `wrapt`, `zipp`).
  - All remaining packages upgraded to their current versions, e.g.:
    - `pysam` 0.18.0 → 0.24.0
    - `duckdb` 0.9.2 → 1.5.3
    - `pyarrow` 14.0.2 → 24.0.0
    - `pandas` 2.1.4 → 2.3.3
    - `numpy` 1.26.4 → 2.4.6
  - Added new packages introduced transitively by the updated `gwas-sumstats-tools`
    (`pandera`, `pydantic`, `typer`, `petl`, `ruamel.yaml`, etc.).
- `gwas-sumstats-tools` is installed from the local source repo in the Docker build until the
  Python 3.12-compatible version is published to PyPI.

## Consequences

**Positive:**

- Python 3.12 receives security and bug-fix updates until October 2028.
- Substantially newer dependency versions reduce the exposure to known CVEs in older releases.
- `duckdb` 1.5.3 and `pyarrow` 24 bring meaningful query-performance improvements.
- The dependency tree is smaller and cleaner (12 obsolete packages removed).

**Negative / risks:**

- The Docker build temporarily requires `gwas-sumstats-tools` to be present as a sibling
  directory in the build context (`docker build` must be run from the parent directory) until
  the updated package is published to PyPI.
- `datrie 0.8.3` has no pre-built wheel for Python 3.12 and must be compiled from source;
  `build-essential` is already present in the Docker image so this is handled automatically,
  but it adds a small amount of build time.
- Any code relying on removed packages or deprecated Python 3.9-era behaviour will need to
  be updated.
