# 1. Upgrade to Python 3.13

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
- `gwas-sumstats-tools`, a first-party dependency, requires Python ≥ 3.13
  (`requires-python = ">=3.13"`), making any version below 3.13 incompatible with the latest
  published version.

## Decision

Upgrade the project to **Python 3.13** and update all dependencies to their latest compatible
versions.

Changes made:

- `Dockerfile`: base image changed from `python:3.9-slim-bookworm` to `python:3.13-slim-bookworm`.
- `environments/conda_environment.yml`: `python` updated to `3.13`.
- `environments/requirements.txt`: fully regenerated from a clean Python 3.13 install.
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

## Consequences

**Positive:**

- Python version is consistent with `gwas-sumstats-tools`, eliminating version-mismatch risk.
- Python 3.13 receives security and bug-fix updates until October 2029.
- Substantially newer dependency versions reduce the exposure to known CVEs in older releases.
- `duckdb` 1.5.3 and `pyarrow` 24 bring meaningful query-performance improvements.
- The dependency tree is smaller and cleaner (12 obsolete packages removed).
- Python 3.13 includes interpreter performance improvements (continued work from the specialising
  adaptive interpreter introduced in 3.11).

**Negative / risks:**

- Any package not yet publishing a wheel for Python 3.13 will need to be compiled from source;
  `build-essential` is present in the Docker image to handle this automatically.
- `datrie 0.8.3` has no pre-built wheel for Python 3.13 and must be compiled from source.
- Python 3.13 removes several long-deprecated standard-library modules — any indirect use via
  dependencies would surface as import errors and require a dependency update.
- Any code relying on removed packages or deprecated Python 3.9-era behaviour will need to
  be updated.
