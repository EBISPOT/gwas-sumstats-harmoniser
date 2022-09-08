# gwas-sumstats-harmoniser

GWAS Summary Statistics Data Harmonisation pipeline aims to bring the variants to the desired genome assembly and then harmonises variants to match variants in reference data.

The harmonisation process is the following:
      a) Mapping variant IDs to locations*.
      b) find the orientation of the variants*. 
      c) resolve RSIDs from locations and alleles 
      d) orientate the variants to the reference strand.

## Prerequisites 
### Memory
To optimise for speed, the pipeline allocates 28GB for the mapping variants on all chromosomes. You could lower this to around 20GB, but expect failures anywhere lower. For any user who wants to run on the local computer, we suggest running chr22 for testing purposes.
### Packages
Compulsory:
1. python3
2. [HTSlib for tabix](http://www.htslib.org/download/)
3. [Nextflow (>=21.04.0)](https://www.nextflow.io/docs/latest/getstarted.html#installation)

optional:
1. conda  
2. or Singularity (2.3.x or higher)
3. or Docker

## 1. Installation
First clone this repository:
```
git clone -b nextflow --recurse-submodules https://github.com/EBISPOT/gwas-sumstats-harmoniser.git 
cd gwas-sumstats-harmoniser
```
If you decide to run on conda, please run:
```
conda create -n harmonisation python=3.9
conda activate harmonisation
pip install -r requirements.txt
```
Change the permission of all scripts in the bin folder:
```
chmod -R 777 ${script_folder}/bin
```

## 2. Reference preparation

The resource bundle is a collection of standard files for harmonizaing GWAS summary statistics data. We support the ensembl variants VCF reference (hg38, dbSNP 151) and synonyms table . These files can be directly downloaded from our [FTP](http:) server.

Users can also prepare their own reference:
```
nextflow main.nf --reference -c ./config/reference.config
```
Path of the resources can be changed in the file ./config/reference.config

## 3. Running the pipeline:
### 3.1 General users

step1: prepare input file:
* Files are correctly formatted using the validator.
* The name must follow the convention <any identifier>_<genome assembly number>.tsv e.g. my_summary_stats_37.tsv (37 denotes the genome assembly of the data in the file is hg19 or GRCh37)

step2: run the pipeline:
Harmonising one file:

```
nextflow main.nf --reference -c ./config/lsf.config --harm --file Full_path_of_file_to_be_harmonized (--list path_of_list.txt)
```
or harmonising a batch of files in list.txt file, which is a txt file that each row is a full path of tsv files to be harmonised. 
```
nextflow main.nf --reference -c ./config/lsf.config --harm --list path_of_list.txt
```

All results will be stored in the current working directory.

### 3.2 GWAS catalog users

We constructed a customized pipeline for GWAS catalog daily running. This pipeline will automatically capture the first 200 studies from all_harm_folder and move files to the current working folder. Then harmonise all files and store intermediate files in the working folder. Successfully harmonised files' results (*.h.tsv.gz, *.tsv.gz.tbi and *.running.log) will be moved to ftp_folder, and files that failed to be harmonised are moved to failed_folder. All paths can be customized in ./config/gwascatalog.config files.

```
nextflow main.nf --reference -c ./config/gwascatalog.config --gwascatalog
```

### 3.3. Other options:
* -resume (Execute the script using the cached results)
* -with-docker 'docker://ebispot/gwas-sumstats-harmoniser:latest' (run docker)
* -with-singularity 'docker://ebispot/gwas-sumstats-harmoniser:latest' (run Singularity)
  
# 6. The harmonisation process is the following:

### 1. Mapping variant IDs to locations
  a. Update base pair location value by mapping variant ID using latest Ensembl release, or
  b. if above not possible, liftover base pair location to latest genome build, or
  c. if above not possible, remove variant from file.

### 2. Orientation (Open Targets project)
a. Using chromosome, base pair location and the effect and other alleles, check the orientation of all non-palindromic variants against Ensembl VCF references to detemine consensus:
  * forward
  * reverse
  * mixed
b. If the consensus is 'forward' or 'reverse', the following harmonisation steps are performed on the palindromic variants, with the assumption that they are orientated according to the consensus, otherwise palindromic variants are not orientated.

c. Using chromosome, base pair location and the effect and other alleles, query each variant against the Ensembl VCF reference to harmonise as appropriate by either:
  * keeping record as is because:
        it is already correctly orientated
        it cannot be orientated
  * orientating to reference strand:
        reverse complement the effect and other alleles
  * flipping the effect and other alleles
        because the effect and other alleles are flipped in the reference
        this also means the beta, odds ratio, 95% CI and effect allele frequency are inverted
  * a combination of the orientating and flipping the alleles.

d.The result of the orientation is the addition of a set of new fields for each record (see below). A harmonisation code is assigned to each record indicating the harmonisation process that was performed (note that currently any processes involving 'Infer strand' are not being used).

### 3. Filtering and QC
  * Variant ID is set to variant IDs found by step (2).
  * Records without a valid value for variant ID, chromosome, base pair location and p-value are removed.
