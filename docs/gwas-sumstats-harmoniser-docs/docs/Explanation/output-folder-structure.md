---
sidebar_position: 2
---
# Final files description
### Harmonised sumstat result
<details>
  <summary> Example of the harmonisation result <code>random_name.h.tsv.gz</code></summary>
  | chromosome | base_pair_location | effect_allele | other_allele | beta        | standard_error | effect_allele_frequency | p_value | rsid          | info                | zscore | hm_coordinate_conversion | odds_ratio | hm_code | variant_id           |
  |------------|---------------------|----------------|---------------|-------------|----------------|-------------------------|---------|---------------|---------------------|--------|-------------------------|------------|---------|----------------------|
  | 1          | 758351              | G              | A             | 0.01       | 0.00806496     | 0.997221                | 0.1     | rs12238997    | ref_rs12238997      | 0.02   | lo                      | 0.03       | 12      | 1_758351_A_G        |
  | 1          | 1000013             | GCCACGGG       | G             | 0.01       | 0.00806496     | 0.002779999999999976    | 0.1     | rs1469404497  | ref_rs1469404497_norsid_flipped | -0.02  | lo                      | -33.333333333333336 | 11      | 1_1000013_G_GCCACGGG |
  | 1          | 1000095             | C              | CGC           | 0.01       | 0.00806496     | 0.997221                | 0.1     | rs1014128468  | ref_rs1014128468_norsid_flipped | -0.02  | lo                      | -33.333333333333336 | 13      | 1_1000095_CGC_C      |
  | 22         | 15925047            | A              | G             | -0.00477642 | 0.0164749      | 0.089851                | 0.77    | rs376238049   | ref_rs376238049     | 0.02   | lo                      | 0.03       | 12      | 22_15925047_G_A     |

</details>
* The harmonised result file represents the harmonised [mandatory columns](https://www.ebi.ac.uk/gwas/docs/summary-statistics-format) in a specific order, followed by the remaining columns from the original file in their original order.
* All values in this file reflect the harmonised results.
* All the variants in this file are sorted by chr and position and compressed using bgzip
* In addition to the columns from the original file, two extra columns are included:
  * `hm_coordinate_conversion` Describes how this variant was mapped to the target genome.
  * `harmonisation code` A code assigned to each record indicating the harmonisation process that was applied.
  * Please refer to this [page](../Reference-guide/Hm_code.md) for more detailed information.

### YAML file for harmonised sumstat
<details>
  <summary>An example of metadata YAML file for the harmonised data file.<code>random_name.h.tsv.gz-meta.yaml</code></summary>
  ```Text
  coordinate_system: 1-based
  data_file_md5sum: 0e6ae204cb1ac0198d947b004e78e080
  data_file_name: random_name.h.tsv.gz
  date_metadata_last_modified: 2024-10-18
  file_type: GWAS-SSF v1.0
  genome_assembly: GRCh38
  harmonisation_reference: ftp://ftp.ensembl.org/pub/release-95/fasta/homo_sapiens/dna/
  is_harmonised: true
  is_sorted: true
  ```
</details>
This YAML file provides metadata about the harmonised result, including:

* Whether the file is harmonised and sorted
* The reference used for harmonisation
* The current genome build and coordinate system
* The md5sum for file integrity verification

### Tabix file for final harmonised sumstat
A tabix index file of the harmonisation result for quick data retrieval purposes

### Running log summary the whole harmonisation process
<details>
  <summary><code>random_name.running.log</code></summary>
  ```text
  ################################################################

HARMONISATION RUNNING REPORT

################################################################




1. Pipeline details

    A. Pipeline Version: 0.1.0

    B. Running date: Aug 1 2024

    C. Input file: GCST90132222_buildGRCh37.tsv.gz

################################################################




2. Reference data

##source=ensembl;version=95;url=http://vertebrates.ensembl.org/homo_sapiens

##reference=ftp://ftp.ensembl.org/pub/release-95/fasta/homo_sapiens/dna/

##ID=dbSNP_151,Number=0,Type=Flag,Description="Variants (including SNPs and indels) imported from dbSNP"

################################################################




3. Mapping result

0.609759% (132485 sites out of 21727440) were dropped because they could not be mapped. 
99.3902% (21594955 sites) were carried forward.


################################################################



4. Palindromic SNPs

palin_mode: forward

Direction of palindromic SNPs inferred as forward by establishing consensus direction of 10% of all sites (forward sites ratio =0.9990667294151884).

################################################################



5. Successfully harmonised variants

95.29% ( 20577937 of 21594955 ) sites successfully harmonised.

hm_code	Number	Percentage	Explanation
10	17532145	81.19%	Forward strand; Correct orientation; Already harmonised
11	54665	0.25%	Forward strand; Flipped orientation; Requires harmonisation
12	14832	0.07%	Reverse strand; Correct orientation; Already harmonised
13	1873	0.01%	Reverse strand; Flipped orientation; Requires harmonisation
5	2967202	13.74%	Palindromic; Assume forward strand; Correct orientation; Already harmonised
6	7220	0.03%	Palindromic; Assume forward strand; Flipped orientation; Requires harmonisation

################################################################



6. Failed harmonisation

4.71% ( 1017018 of 21594955 ) sites failed to harmonise.

hm_code	Number	Percentage	Explanation
15	1006754	4.66%	No matching variants in reference VCF; Cannot harmonise
14	10224	0.05%	Required fields are not known; Cannot harmonise
16	40	0.00%	Multiple matching variants in reference VCF (ambiguous); Cannot harmonise

################################################################



7. Overview

Result	SUCCESS_HARMONIZATION

  ```
</details>
The running log file provides detailed information about the harmonisation process, including:

* The pipeline version and the date of harmonisation
* The reference VCF file and dbSNP version used
* A summary of the genome build mapping results, reporting the number and percentage of variants dropped during this step
* The orientation inferred for palindromic variants and the strand consensus ratio
* The number and percentage of variants successfully harmonised for each `hm_code`
* The number and percentage of variants that failed to be harmonised for each `hm_code`

:::info[Harmonised result before April 2023]
Starting in April 2023, with the release of the GWAS-SSF standard by the GWAS-Catalog, we began retaining only the harmonised results in the final `*.h.tsv` file to ensure consistency with the input file and reduce redundancy. 

For files harmonised before this date, you will see two outputs for each summary statistic: one harmonised result (`*.h.tsv.gz`) and one YAML file (`*.h.tsv.gz-meta.yaml`). The harmonisation process remains the same, but there is a slight difference in how data is represented in the `*.h.tsv.gz`. In these older harmonised files, the harmonised values are listed in columns starting with `hm_`, such as `hm_chromosome`.
:::
