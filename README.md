# gwas-sumstats-harmoniser

GWAS Summary Statistics Data Harmonisation pipeline aims to bring the variants to the desired genome assembly and then harmonises variants to match variants in reference data.

The harmonisation process is the following:
* a) Mapping variant IDs to locations.
* b) Find the orientation of the variants. 
* c) Resolve RSIDs from locations and alleles 
* d) Orientate the variants to the reference strand.

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

The resource bundle is a collection of standard files for harmonisaing GWAS summary statistics data. We support the ensembl variants VCF reference (hg38, dbSNP 151) and synonyms table . These files can be directly downloaded from our [FTP](http:) server.

Users can also prepare their own reference:
```
nextflow main.nf --reference -c ./config/reference.config
```
Path of the resources can be changed in the file ./config/reference.config

## 3. Running the pipeline:
### 3.1 General users

Step1: Prepare input file:
* Files are correctly formatted using the validator.
* The name must follow the convention <any identifier>_<genome assembly number>.tsv e.g. my_summary_stats_37.tsv (37 denotes the genome assembly of the data in the file is hg19 or GRCh37)

Step2: Run the pipeline.
  
Harmonising one file:

```
nextflow main.nf --reference -c ./config/lsf.config --harm --file Full_path_of_file_to_be_harmonised (--list path_of_list.txt)
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
  
# 4. Harmonisation steps:
More information about the harmonisation process refers to [GWAS catalog documents](https://www.ebi.ac.uk/gwas/docs/methods/summary-statistics)
