---
sidebar_position: 7
---

# (Optional) Customise the Configuration

The pipeline behaviour can be fine-tuned through a set of Nextflow configuration files in the `config/` directory. This is especially useful in batch mode, where you want to set persistent defaults instead of repeating parameters on every run.

---

## 1. Singularity File System Mounting

Some users have encountered Singularity errors on HPC systems caused by file system mounting restrictions. To resolve this, bind the harmonisation resources directory explicitly in `nextflow.config`:

```groovy title="nextflow.config"
singularity {
    enabled    = true
    runOptions = "-B </absolute/path/to/harmonisation_resources>"
}
```

Thanks to Olivier Bakker for identifying this issue and providing the solution. See [issue #124](https://github.com/EBISPOT/gwas-sumstats-harmoniser/issues/124) for more context.

---

## 2. Pipeline Parameters (`default_params.config`)

Controls the core harmonisation parameters.

```groovy title="config/default_params.config"
params {
    to_build  = '38'
    chrom     = ['1','2','3','4','5','6','7','8','9','10','11','12',
                 '13','14','15','16','17','18','19','20','21','22','X','Y','MT']
    threshold = '0.99'
    version   = 'v1.1.10'
}
```

| Parameter   | Description                                                                   | Default   |
|-------------|-------------------------------------------------------------------------------|-----------|
| `to_build`  | Target genome build — `38` (GRCh38) or `37` (GRCh37).                        | `38`      |
| `chrom`     | Chromosomes to process. Remove entries to restrict to specific chromosomes.   | All       |
| `threshold` | Minimum proportion of variants that must map successfully to pass QC.         | `0.99`    |
| `version`   | Version of the pipeline reference data.                                       | `v1.1.10` |

:::info
Any parameter can be overridden at runtime on the command line:
```bash
nextflow run EBISPOT/gwas-sumstats-harmoniser --to_build 37 --threshold 0.95
```
:::

---

## 3. Memory and Time (`basic.config`)

Memory and time limits for each pipeline step are defined in [`config/basic.config`](https://github.com/EBISPOT/gwas-sumstats-harmoniser/blob/master/config/basic.config).

| Mode | Memory needed |
|------|--------------|
| Test mode | ~3 GB — suitable for local testing and debugging |
| Full run (all chromosomes) | ≥28 GB — recommend running on HPC or cloud |

:::caution
Do not reduce memory settings when processing all chromosomes. Requirements are driven by reference file size, and insufficient memory will cause the pipeline to fail. Adjustments are only safe when running a subset of chromosomes.
:::

---

## 4. Error Handling

By default, the pipeline uses `ignore_error.config` — failed processes are skipped after retries and the run continues. Pass `--terminate_error` to use `exit_error.config` and stop the pipeline on failure instead:

```bash
nextflow run EBISPOT/gwas-sumstats-harmoniser --terminate_error
```

The retry-triggering exit codes and maximum retries can be adjusted directly in the config files.

```groovy title="config/ignore_error.config"
process {
    errorStrategy = { task.exitStatus in ((130..145) + 104) ? 'retry' : 'ignore' }
    maxRetries    = 5
    maxErrors     = '-1'
}
```

```groovy title="config/exit_error.config"
process {
    errorStrategy = { task.exitStatus in ((130..145) + 104) ? 'retry' : 'terminate' }
    maxRetries    = 5
    maxErrors     = '-1'
}
```
---

For all available Nextflow configuration options, see the [Nextflow documentation](https://www.nextflow.io/docs/latest/config.html).
