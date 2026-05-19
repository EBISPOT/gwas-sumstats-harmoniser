---
sidebar_position: 1
---
# Output folder structure explanation
``` text title="random_name"
.
├── 1_map_to_build
│   ├── 1.merged
│   ├── ......
│   ├── 22.merged
│   ├── random_name.tsv-meta.yaml
│   └── unmapped
├── 2_ten_sc
│   ├── ten_percent_chr1.sc
│   ├── .......
│   └── ten_percent_chr22.sc
├── 4_harmonization
│   ├── chr1.merged.hm
│   ├── chr1.merged.log.tsv.gz
│   ├── .......
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
## Genome Build Mapping outputs
This step lifts variants to the desired genome assembly (GRCh38) using the process `./modules/local/map_to_build.nf`, generating the following outputs:

- `chr*.merged`: Contains variants with their `chromosome` and `base_pair_location` updated to the target genome build.
- `unmapped`: Collects all variants (rows) that failed to be mapped to the target genome build.

## Orientation of palindromic variants outputs
This step is to infer palindromic variants' strand orientation using a strand consensus approach. It contains two process: `./modules/local/ten_percent_counts.nf` and `./modules/local/ten_percent_counts_sum.nf`
- `./ten_sc/ten_percent_chr*.sc`: For variants in each chromosome, this output summarises the number of:
     - Forward strand variants
     - Reverse strand variants
     - Palindromic variants
     - No VCF record found
     - Invalid variants for harmonisation
- `ten_percent_total_strand_count.tsv`: This file provides an overview across all chromosomes, detailing the counts of each variant type. It calculates the strand consensus ratio and infers the mode for palindromic variants:
   - forward: Infers that the palindromic variant is on the forward strand
   - reverse: Infers that the palindromic variant is on the reverse strand
   - drop: Indicates that the strand of the palindromic variant cannot be inferred, and these variants are dropped from harmonisation.

## Harmonising the variant outputs
This process aligns each variant with the reference and makes necessary changes to the corresponding values. It is the product of the process `./modules/local/harmonization.nf`
- `harmonization/chr*.merged.hm`: Contains the harmonized results for each chromosome.
- `harmonization/chr*.merged.log.tsv.gz`: This log file summarising the number of variants for each `hm_code` that appears in this chromosome. Please refer to this [file](../Introduction/Harmonising-the-variants.mdx) for more information about the `hm_code`.

## Quality control outputs
The QC process involves filtering out variants that lack valid values in essential columns. It includes results from the process `.modules/local/qc.nf`.
- `qc/harmonised.tsv`: This file includes the harmonised results from all chromosomes.
- `qc/harmonised.qc.tsv`: This file includes the harmonised results of variants without missing values in [essential columns](../Tutorials/Preparing-Input-Files#data-requirement).
- `qc/report.txt`: This file documents the rows that were removed during this QC step.

## Final outputs
The `final` folder contains well-compressed and organised final outputs:
- `final/random_name.h.tsv.gz`: Bgzip-compressed and sorted harmonisation results from all chromosomes.
- `final/random_name.h.tsv.gz-meta.yaml`: Updated YAML file for `final/random_name.h.tsv.gz`.
- `final/random_name.h.tsv.gz.tbi`: Tabix file for `final/random_name.h.tsv.gz`
- `final/random_name.running.log`: Summary log recording information about the pipeline, reference, genome build results, inferred strand of palindromic SNPs, and the number as well as percentage of variants that were successfully harmonised or failed.
