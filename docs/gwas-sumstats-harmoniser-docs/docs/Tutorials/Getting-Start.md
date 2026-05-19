---
sidebar_position: 1
---
# Getting Started

## Install necessary dependencies

:::warning
To successfully run the `gwas-sumstats-harmoniser`, it is crucial to have the following dependencies installed:

* Linux or macOS
* HTSlib for tabix
* Nextflow
* Docker, Singularity, or Anaconda
:::

1. Install [HTSlib](https://www.htslib.org/download/)
```bash
wget https://github.com/samtools/htslib/releases/download/1.21/htslib-1.21.tar.bz2
cd htslib-1.21
./configure --prefix=/where/to/install
make
make install
export PATH=/where/to/install/bin:$PATH 
```
* Confirm the `htslib` installation by
```text
$ tabix

Version: 1.9
Usage:   tabix [OPTIONS] [FILE] [REGION [...]]
```

2. Install [Nextflow](https://www.nextflow.io/docs/latest/install.html)
```bash
java -version # Java v8+ required
# openjdk 11.0.13 2021-10-19
curl -fsSL get.nextflow.io | bash
chmod +x nextflow
mv nextflow ~/bin/
```
* Confirm the the `nextflow` installation by
```text 
$ nextflow info

Version: 24.10.0 build 5928
Created: 27-10-2024 18:36 UTC (18:36 BST)
System: Linux 4.18.0-513.5.1.el8_9.x86_64
Runtime: Groovy 4.0.23 on Java HotSpot(TM) 64-Bit Server VM 17.0.4.1+1-LTS-2 Encoding: UTF-8 (UTF-8)
```

3. Next, install [Singularity](https://docs.sylabs.io/guides/3.0/user-guide/installation.html), [Docker](https://docs.docker.com/engine/install/) or [Anaconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html). Note that using Singularity or Docker is recommended.

* Before starting the installation, it's a good idea to check if any of these tools are already installed on your system:
```text
$ singularity --version
singularity-ce version 4.1.4-1.el8

$ docker --version
Docker version 27.2.0, build 3ab4256

$ conda --version
conda 23.11.0
```

## Run your first harmonisation pipeline
To run your first harmonization pipeline, execute the following command:
``` bash
nextflow run  EBISPOT/gwas-sumstats-harmoniser -r $release_version -profile test,singularity
```
🚨 If you did not choose to install Singularity, remember to replace `singularity` with `docker` or `conda`.

Once Nextflow starts running:
1. It will download the `gwas-sumstats-harmoniser` pipeline from Github into the global cache `~/.nextflow/assets`. (Please note that in the nextflow, `-r` determines which version of the pipeline to use, for example, "v1.1.10"; while `--version` will only decide what is recorded in the  [`running.log`](../Explanation/output-folder-structure#running-log-summary-the-whole-harmonisation-process) file.)
2. It will pull the Docker image from [Docker Hub](https://hub.docker.com/r/ebispot/gwas-sumstats-harmoniser) and built Singularity container.
3. Using the input files `random_name.tsv`,`random_name.tsv-meta.yaml`  along with a small test reference file provided in the ` ~/.nextflow/assets/EBISPOT/gwas-sumstats-harmoniser/test_data`, it will execute the pipeline.
4. Once the pipeline executes, you can monitor the progress in the terminal, which may look like this:
``` bash
 N E X T F L O W   ~  version 24.04.2

Launching `https://github.com/EBISPOT/gwas-sumstats-harmoniser` [maniac_mestorf] DSL2 - revision: 67198bb9e7

Harmonizing the file ~/.nextflow/assets/EBISPOT/gwas-sumstats-harmoniser/test_data/random_name.tsv
executor >  local (13)
[9a/05e067] NFC…ATALOGHARM:major_direction:map_to_build (random_name) | 1 of 1 ✔
[cc/fef59c] NFC…ajor_direction:ten_percent_counts (random_name_chr22) | 2 of 2 ✔
[46/626f6f] NFC…:major_direction:ten_percent_counts_sum (random_name) | 1 of 1 ✔
[1c/b92ff2] NFC…_direction:generate_strand_counts (random_name_chr22) | 2 of 2 ✔
[b7/3f81cb] NFC…major_direction:summarise_strand_counts (random_name) | 1 of 1 ✔
[67/216e41] NFC…TALOGHARM:main_harm:harmonization (random_name_chr22) | 2 of 2 ✔
[a9/336d51] NFC…OGHARM:main_harm:concatenate_chr_splits (random_name) | 1 of 1 ✔
[64/8a8fec] NFC…HARM:GWASCATALOGHARM:quality_control:qc (random_name) | 1 of 1 ✔
[6e/fb619c] NFC…GHARM:quality_control:harmonization_log (random_name) | 1 of 1 ✔
[48/1ae6d4] NFC…OGHARM:quality_control:update_meta_yaml (random_name) | 1 of 1 ✔
[chr1, chr22,  is being harmonized]
```

In your current directory, you will find a folder named `random_name` that contains all intermediate files and the final result.

```text
./random_name/
├── 1_map_to_build
│   ├── 1.merged
│   ├── 22.merged
│   ├── random_name.tsv-meta.yaml
│   └── unmapped
├── 2_ten_sc
│   ├── ten_percent_chr1.sc
│   └── ten_percent_chr22.sc
├── 4_harmonization
│   ├── chr1.merged.hm
│   ├── chr1.merged.log.tsv.gz
│   ├── chr22.merged.hm
│   └── chr22.merged.log.tsv.gz
├── 5_qc
│   ├── harmonised.qc.tsv
│   ├── harmonised.tsv
│   └── report.txt
├── final
│   ├── random_name.h.tsv.gz
│   ├── random_name.h.tsv.gz-meta.yaml
│   ├── random_name.h.tsv.gz.tbi
│   └── random_name.running.log
└── ten_percent_total_strand_count.tsv
```
This output confirms that the pipeline has been successfully executed and is ready to process larger real datasets.
