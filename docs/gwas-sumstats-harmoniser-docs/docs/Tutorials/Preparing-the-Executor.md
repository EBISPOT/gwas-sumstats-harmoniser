---
sidebar_position: 4
---
# Preparing the Executor
Nextflow supports running pipelines on a variety of execution environments, including local machines, HPC systems, and cloud platforms. The default executor for this pipeline is `SLURM,` but you can easily switch between different environments by modifying the executor settings in the `executor.config` file.

#### Example: Setting LSF as the Executor
To configure the pipeline to run with the LSF executor, update the `process.executor` as shown below:

```groovy title="config/executor.config"
process{
    executor = 'slurm'
    memory = '28GB'
    time = '2d'
}
```
For running the pipeline with other supported executors such as `PBS`, `k8s`, `AWS Batch`,`Google Cloud Batch` that Nextflow currently supports, please refer to the [Nextflow documentation](https://www.nextflow.io/docs/latest/executor.html#lsf) for detailed instructions on configuring the desired execution environment.

# Resource requirement:
* **Test Mode**: The pipeline's test mode requires as little as 3G of memory, making it ideal for running on your local computer to validate the setup or debug issues.

* **Real Data Processing**: When processing whole chromosomes with real data, you'll need at least **28G** of memory. For such resource-intensive tasks, it is recommended to run the pipeline on an HPC or cloud system to ensure optimal performance.

