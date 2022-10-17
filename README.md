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
1. [HTSlib for tabix](http://www.htslib.org/download/)
2. [Nextflow (>=22.04.0)](https://www.nextflow.io/docs/latest/getstarted.html#installation)


optional:
1. conda  
2. or Singularity (3.7.x or higher)
3. or Docker

## 1. Download the pipeline and test it on a minimal dataset with a single command:
```
nextflow run  EBISPOT/gwas-sumstats-harmoniser -profile test,<docker/singularity/conda>
```
This repository is stored in the Nextflow home directory, that is by default the $HOME/.nextflow path, and thus will be reused for any further executions.

## 2. Reference preparation

The resource bundle is a collection of standard files for harmonisaing GWAS summary statistics data. We support the ensembl variants VCF reference (hg38, dbSNP 151) and synonyms table . These files can be directly downloaded from our [FTP](http:) server.

Users can also prepare your own reference:
```
nextflow run  EBISPOT/gwas-sumstats-harmoniser \
--reference \
--ref 'full path to store reference' \
-profile cluster,singularity/conda (running on the cluster) or -profile standard,docker/conda  (running locally)
```
Default reference were originally downloaded from 
```
remote_vcf_location = 'ftp://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens'
remote_ensembl_variation = 'ftp://ftp.ensembl.org/pub/release-100/mysql/homo_sapiens_variation_100_38'
```
Other version can de defined by using `--remote_vcf_location` and `--remote_ensembl_variation`.
If you want to only run specific chromsomes, `--chrom 22` or `--chromlist 22,X,Y` are available to set it


## 3. Running the pipeline:
### 3.1 General users

Step1: Prepare input file:
* Files are correctly formatted using the validator.
* The name must follow the convention <any identifier>_<genome assembly number>.tsv e.g. my_summary_stats_37.tsv (37 denotes the genome assembly of the data in the file is hg19 or GRCh37)

Step2: Run the pipeline.
  
Harmonising one file:

```
nextflow run  EBISPOT/gwas-sumstats-harmoniser \
--ref 'full path to store reference' \
--harm \
--file Full_path_of_the_file_to_be_harmonised or --list path_of_list.txt \
-profile cluster,singularity/conda or -profile standard,docker/conda
```
Harmonising a batch of files in list.txt file, which is a txt file that each row is a full path of tsv files to be harmonised. 

All results will be stored in the current working directory.

### 3.2 GWAS catalog users

We constructed a customized pipeline for GWAS catalog daily running. This pipeline will automatically capture the first 200 studies from all_harm_folder and move files to the current working folder. Then harmonise all files and store intermediate files in the working folder. Successfully harmonised files' results (*.h.tsv.gz, *.tsv.gz.tbi and *.running.log) will be moved to ftp_folder, and files that failed to be harmonised are moved to failed_folder. All paths can be customized in the file $HOME/.nextflow/assets/EBISPOT/gwas-sumstats-harmoniser/config/gwascatalog.config.
```
nextflow run  EBISPOT/gwas-sumstats-harmoniser \
--ref 'full path to store reference' \
--gwascatalog \
-profile cluster,singularity/
```

### 3.3. Other options:
* -resume (Execute the script using the cached results)

### 3.4. Other Executors:
To run the harmonisation pipeline with other executors, user can import the customized config files using `--custom.config path_of_custom.config` and using `-profile` to select the process configuration strategy.
 ```
profiles {

    cluster {
        process.executor = 'sge'
        process.queue = 'long'
        process.memory = '10GB'
    }

    cloud {
        process.executor = 'cirrus'
        process.container = 'cbcrg/imagex'
        docker.enabled = true
    }

}
 ```
# 4. Harmonisation steps:
More information about the harmonisation process refers to [GWAS catalog documents](https://www.ebi.ac.uk/gwas/docs/methods/summary-statistics)
