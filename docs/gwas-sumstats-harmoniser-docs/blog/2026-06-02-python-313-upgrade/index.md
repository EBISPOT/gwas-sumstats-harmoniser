---
slug: python-313-upgrade
title: Upgraded to Python 3.13
authors: [yueji]
tags: [dependencies, python]
---

The pipeline runtime has been upgraded from **Python 3.9 to Python 3.13**, aligning it with `gwas-sumstats-tools` and bringing in a substantially refreshed dependency stack.

<!-- truncate -->

## Why upgrade?

Python 3.9 reached end-of-life in October 2025 and no longer receives security or bug-fix patches. At the same time, `gwas-sumstats-tools` — the first-party package that handles summary-statistic validation and formatting — was updated to require Python ≥ 3.13. Keeping the harmoniser on 3.9 would have created an incompatibility with the latest published version of that package.

## What changed?

- **Docker base image** updated from `python:3.9-slim-bookworm` to `python:3.13-slim-bookworm`.
- **Conda environment** updated to `python=3.13`.
- **All dependencies** regenerated from a clean Python 3.13 environment. Key upgrades include:

| Package    | Old version | New version |
|------------|-------------|-------------|
| `pysam`    | 0.18.0      | 0.24.0      |
| `duckdb`   | 0.9.2       | 1.5.3       |
| `pyarrow`  | 14.0.2      | 24.0.0      |
| `pandas`   | 2.1.4       | 2.3.3       |
| `numpy`    | 1.26.4      | 2.4.6       |

- 12 stale transitive packages removed (`appdirs`, `attrs`, `chardet`, `docutils`, and others no longer in the resolution graph).
- New packages added by the updated `gwas-sumstats-tools` (`pandera`, `pydantic`, `typer`, `petl`, `ruamel.yaml`, etc.).

## What do I need to do?

If you run the pipeline via **Docker or Singularity** (recommended), no action is required — the updated image will be pulled automatically.

If you use the **Conda profile**, recreate your environment from the updated `environments/conda_environment.yml`:

```bash
conda env remove -n gwas_harm
conda env create -f environments/conda_environment.yml
conda activate gwas_harm
```

## Notes

- `datrie 0.8.3` has no pre-built wheel for Python 3.13 and is compiled from source at install time. This is handled automatically by `build-essential` in the Docker image and adds a small amount of build time.
- Python 3.13 removes several long-deprecated standard-library modules. If you encounter import errors in third-party dependencies, please open an issue on [GitHub](https://github.com/EBISPOT/gwas-sumstats-harmoniser/issues).
