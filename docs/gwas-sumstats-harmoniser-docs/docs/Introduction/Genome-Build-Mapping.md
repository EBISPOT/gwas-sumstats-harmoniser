---
sidebar_position: 2
---

# Genome Build Mapping

The first step in harmonizing variant data is updating the genomic coordinates to the desired assembly (GRCh38). The pipeline follows a systematic approach to ensure high-confidence mapping of each variant's position:

### Step 1: Mapping by rsID using Ensembl (v95)
 The pipeline first attempts to update the variant's base pair location by mapping its rsID to the variants from the Ensembl reference database. If successful, the field `hm_coordinate_conversion` is set to `rs` for these variants in the output file [`${chr}.merges`](../Explanation/output-file-explanation#genome-build-mapping-outputs), indicating that the variant's position was determined through rsID-based mapping.

### Step 2: Liftover to the latest genome build
 For variants where rsID mapping is not possible, the pipeline uses the UCSC LiftOver tool to lift the coordinates from an older genome build to GRCh38. If successful, the field `hm_coordinate_conversion` is set to `lo`, indicating that the base pair location was updated by lifting over the original coordinates.
 
### Step 3: Variant removal
 If neither rsID mapping nor liftover is successful, the variant is removed from the file and stored in [`unmapped`](../Explanation/output-file-explanation#genome-build-mapping-outputs) output. This ensures that only high-confidence variants with validated genomic positions are retained in the final dataset.

This process is recorded in the `hm_coordinate_conversion` field in the harmonised data file to provide traceability for how each variant's genomic position was determined. 
