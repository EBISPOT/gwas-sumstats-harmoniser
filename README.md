# gwas-sumstats-harmoniser
GWAS Summary Statistics Data Harmonisation pipeline

The pipeline workflow is managed by `snakemake`, so it can be followed in the [Snakefile](Snakefile). 

This pipeline, brings the variants to the desired genome assembly and then harmonises them. The harmonisation is performed by [sumstat_harmoniser](https://github.com/opentargets/sumstat_harmoniser) which is used to a) find the orientation of the variants, b) resolve RSIDs from locations and alleles and c) orientate the variants to the reference strand.

# Installation

The following are required:

- python3
- [HTSlib](http://www.htslib.org/download/) for tabix

- `git clone --recurse-submodules https://github.com/EBISPOT/gwas-sumstats-harmoniser.git` # clone this repo and submodules
- `cd gwas-sumstats-harmoniser`
- `virtualenv --python=python3 .venv` # create virtual environment
- `source .venv/bin/activate` # activate virtual environment
- `pip install -r requirements.txt`

# Executing the pipeline

### Environment
The recommended way to run the pipeline is on HPC. Follow the snakemake [guidelines](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles) for setting up a profile for your cluster. Although not recommended, it is possible to run locally but understand the memory and disk space requirements. See [here](https://github.com/Snakemake-Profiles/snakemake-lsf) for LSF (bsub) snakemake profile.

### Memory
To optimise for speed, the pipeline allocates 28GB for the mapping rule. You could lower this (edit the Snakefile) to around 20GB, but expect failures anywhere lower.

### Disk space
Allow 20GB for the VCF reference files. If using a local synonyms table (see configuration) allow an additional 70GB (for Ensembl release 100).

### Configuration
Edit the [config.yaml](config.yaml) if you want to change from any of the defaults. It's recommended to set an absoulte path for the `local_resources`. Set `local_synonyms` to `False` if you wish to check variant name synonynms against the Ensembl REST API (not recommended, but if you don't have 70GB free space, you can do this).

### Execution
- The pipeline takes .tsv summary statistics in [this format](https://www.ebi.ac.uk/gwas/docs/methods/summary-statistics)
- The name must follow the convention `<any identifier><genome assembly number>.tsv` e.g. my_summary_stats_37.tsv (37 denotes the genome assembly of the data in the file is hg19 or GRCh37 - see [config](config.yaml) for the assembly table). Note that his number is not the desired build, that is set in the [config](config.yaml).

- Assuming the pipeline will run on an LSF cluster and the file we want to harmonise is called `/path/to/example37.tsv`:
- `snakemake --configfile config.yaml --profile lsf /path/to/example37/harmonised.qc.tsv`
- see [this](example_wrapper.sh) for an idea of how to run

# Pipeline 
- First make sure your files are correctly [formatted](https://www.ebi.ac.uk/gwas/docs/methods/summary-statistics) using the [validator](https://github.com/EBISPOT/gwas-sumstats-validator). 
- Understand the [configuration](config.yaml) and edit to you requirements.
## Steps
### 1. Get references
- Fetch VCF files from Ensembl and convert to .parquet format
### 2. Map variants to desired genome assembly
- Checks variant IDs against those in the references and updates locations. If not found, or no variant IDs are given, liftover is used.
### 3. Determine strand consensus
- Check the the strand of non-palindromic variants by querying their position and alleles against the refereces. If the percentage is >= threshold (default set to 0.99 in [config](config.yaml)), a consensus is made. This value (forward, reverse or drop) is carried to the next step.
### 4. Harmonise associations to reference
- see [here](https://github.com/opentargets/sumstat_harmoniser) for more details.
### 5. QC
- Update any missing variant IDs.
- If given variant ID is different from the one inferred, check if it is a synonym - if not, drop it.
  - The first time this runs, if you have specified to use a local synonyms table in the config, it will need to build that table. 
- Drop any records with missing mandatory fields.





