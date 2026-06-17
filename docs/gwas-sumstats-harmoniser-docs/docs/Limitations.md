---
sidebar_position: 6
---
# Limitations and Considerations

While our tools are designed to deliver robust and efficient solutions for harmonizing summary statistic data, it is important for users to understand certain limitations to make informed decisions when utilizing the harmonized results.

## Liftover-related challenges
### Duplicated variants in harmonised summary statistic

The liftover process, which standardises genomic coordinates to a common genome build like hg38, can inadvertently introduce artefacts such as duplicated variants in the harmonised result. These issues often result from mapping ambiguities when rsIDs are unavailable.

**Example:**
- Records in the raw data

| pairing | variant_id          | chr | pos       | effect_allele | other_allele | freq     | beta       | se         | p   |
|---------|-----------------------|-----|-----------|---------------|--------------|----------|------------|------------|-----|
| a       | 10_17787983_G_A       | 10  | 17787983  | G             | A            | 0.96859  | 0.00290512 | 0.00335139 | 0.39 |
| b       | 10_18034947_G_A       | 10  | 18034947  | G             | A            | 0.606068 | -0.0007883 | 0.00103352 | 0.45 |


- Harmonised result

| pairing | variant_id          | chr | pos       | hm_effect_allele | hm_other_allele | hm_beta     | effect_allele | other_allele | freq     | beta       | se        | p   |
|---------|-----------------------|-----|-----------|------------------|-----------------|-------------|---------------|--------------|----------|------------|------------|-----|
| a       | 10_17745984_G_A       | 10  | 17745984  | G                | A               | 0.00290512  | A             | G            | 0.96859  | 0.00290512 | 0.00335139 | 0.39 |
| b       | 10_17745984_G_A       | 10  | 17745984  | G                | A               | 0.000788316 | A             | G            | 0.606068 | 0.0007883  | 0.00103352 | 0.45 |

- Observations:

    Two distinct variants (a and b) with different effect sizes in the raw data (hg37) were mapped to the same genomic position (10_17745984_G_A) in the harmonized data (hg38). These duplications arise from mapping uncertainties during liftover, particularly when rsIDs are unavailable to guide alignment or when the rsID has been retired.

- Current solution:
     
     When mapping variants to a different genome build, we prioritize using rsIDs when available. As a secondary strategy, we rely on liftover. It is essential to annotate whether positions were mapped via rsID or liftover and implement systematic checks to identify duplicates introduced during harmonization. This information is recorded in the `hm_coordinate_conversion` column, ensuring transparency in the harmonized summary statistics.

## Reference-related challenges
### Failed to harmonise variants
You may notice that the number of variants in the final harmonized dataset is slightly lower than in the raw data. This occurs because some variants cannot be harmonized and are removed during the QC step.

The success or failure of variant harmonization depends on whether the variant exists in the reference database (Ensembl variation database, currently using release 95 with dbSNP 151). During harmonization, each variant is matched against the reference using its chromosome and position (chr:pos). The effect allele (EA) and other allele (OA) are then compared with the reference REF-ALT pairs. This process determines whether the variant is on the forward strand and whether alleles need to be flipped. If a reference record is unavailable, this step cannot be performed, and the variant cannot be harmonized.

A variant may fail to harmonize for the following reasons:
1. No matching record is found in the reference database based on chr:pos.
2. None of the allele pairs in the reference match the input variant.
3. Multiple records exist for the same chr:pos:ref:alt, leading to ambiguity.

- Current solution:

We continuously monitor the harmonisation rate for each study. If a study has a harmonization rate lower than 90%, we make every effort to investigate the cause and reprocess the data if necessary.

We also encourage users to report any concerns they may have about the data.