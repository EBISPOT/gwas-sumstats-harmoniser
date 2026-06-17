---
sidebar_position: 5
---
# Harmonising a single sumstat
---
Running the pipeline requires more than **30 GB** of memory, so we recommend using an HPC cluster or cloud environment for execution. Given that the process may take several hours to complete, we suggest submitting the Nextflow job through a SLURM job. Below is an example of how to run the pipeline 

``` bash title="start_harmonisation.sh"
#!/bin/bash
#SBATCH --time=2-00:00:00        # Job time allocation (2 days)
#SBATCH --mem=5G                 # Memory allocation (5GB)
#SBATCH -J harmonsation          # Job name
#SBATCH --output=$path/harm.out  # Standard output file
#SBATCH --output=$path/harm.err  # Error log file

# Define the reference path
ref_path="full path to store reference"
version='v1.1.10'

# Harmonising single sumstat using Singularity
nextflow run  EBISPOT/gwas-sumstats-harmoniser \
-r $version \
--ref $ref_path \
--harm \
--file Full_path_of_the_file_to_be_harmonised \
-profile executor,singularity
```
Parameters Explained:
| Parameter         | Description                                                                                                   |
|-------------------|---------------------------------------------------------------------------------------------------------------|
| `-r`           | Revision of the project to inspect (either a git branch, tag or commit SHA number).                                                         |
| `--version`           | A parameter that will record the version of the pipeline in the final `runnning.log` file.                                                         |
| `--ref`           | Specifies the path where reference files are stored.                                                         |
| `--harm`          | Executes the harmonisation model to harmonise the summary statistics based on the reference.                 |
| `--file`          | Specifies the full path of the summary statistics file to be harmonised.                                     |
| `--list`          | Specifies [a list](./Running-the-Pipeline-batch#harmonising-files-from-a-list) containing files that need to be harmonised                                     |
| `--chrom`         | Runs the pipeline for a specific chromosome.                                                                  |
| `--chromlist`     | Runs the pipeline for multiple chromosomes.                                                                   |
| `--to_build`      | Specifies the target genome build; the default value is "38" (GRCh38).                                      |
| `--threshold`     | Defines the threshold for strand consensus. The default value is 0.99. For all variants, if either the forward ratio or reverse ratio exceeds **0.99**, palindromic variants will be evaluated as forward (or reverse). |
| `--terminate_error`        | The default error strategy is set to ignore, as the X, Y, and MT chromosomes are often missing if you set running all chromosomes. Alternatively, you can configure the pipeline to terminate immediately if an error occurs.                        |
| `-profile`        | A profile is a set of configuration attributes that can be selected during pipeline execution. Available profiles include `test` (quick run on local), `standard` (local executor), and `executor` (based on `./config/executor.config` with default SLURM). Container options are `conda`, `docker`, and `singularity`. You can view these settings in `./nextflow.config`                         |

You can also edit these parameters in the config file
```groovy title="config/default_params.config"
params {
    to_build='38'
    chrom=['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','MT']
    threshold='0.99'
    version='v1.1.10'
    }
```

The pipeline also supports other [Nextflow CLI options](https://www.nextflow.io/docs/latest/reference/cli.html), such as:
* `-resume`: Executes the script using cached results, allowing you to continue from where the previous execution stopped (e.g., after an error).
* `-with-trace`: Generates a workflow execution trace file for tracking pipeline performance.

<details>
    <summary> Why only 5GB memory is required here </summary>
    
    In Nextflow, the pipeline is made by joining together different processes. The job defined in the `start_harmonisation.sh` script is the initial step to start the pipeline and requires a small amount of memory.
    
    Nextflow manages the execution of each process and allocates the corresponding memory and wall time as specified in the `config/basic.config` file. For example, the map_to_build step, which performs [genome build mapping](../Introduction/Genome-Build-Mapping.md) step, requires 28GB of memory to complete.
    
    Additionally, if a process exits with a status code between 130 and 145, inclusive, or is equal to 104, Nextflow will automatically retry that process up to 5 times, allocating additional memory with each attempt. If it continues to fail due to these exit statuses, you can modify the resource requirements in the configuration file to accommodate the necessary resources.

    <details>
       <summary>resources required in the `Genome Build Mapping` step</summary>
        ```config title="config/basic.config"
        withName:map_to_build {
                memory = { 28.GB * task.attempt }
                time   = { 5.h  * task.attempt }
                publishDir =[ 
                    path:{"${launchDir}/$GCST"},
                    mode: 'copy'
                    ]
            }
        ```
    </details>
</details>


    
