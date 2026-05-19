---
sidebar_position: 3
---
# Orientation of palindromic variants

Palindromic variants are genetic variants (such as A/T or G/C SNPs) that appear identical on both the forward and reverse DNA strands, making it difficult to distinguish their true orientation. To ensure that these variants are correctly aligned during harmonisation, the pipeline employs a method to infer their strand orientation using a strand consensus approach.

The pipeline uses the following method to infer the orientation of palindromic variants:

### Step 1: Strand Count

* First, 10% of non-palindromic sites are randomly selected. For these sites, the alleles (both effect and other alleles) are compared with the reference and alternative alleles in the Ensembl VCF reference. This comparison helps determine whether the variants align with the forward strand (same as the reference) or the reverse strand (reverse complement of the reference).

### Step 2: Calculating strand consensus
* Based on this comparison, the strand consensus rate is calculated as either:     
   * `forward/(forward + reverse)`, or 
   * `reverse/(forward + reverse)`.

   This rate reflects the proportion of variants aligning to the forward strand (or reverse strand).

### Step 3: Inferring the strand of palindromic variants
The consensus rate is then used to infer the orientation of palindromic variants. To minimize sampling bias and ensure accurate orientation, the pipeline applies the following thresholds:
  - If the consensus rate is ≥ 0.995
    - The palindromic variants are inferred to be aligned on the forward (or reverse) strand. The harmonisation proceeds accordingly for these variants.
  - If the consensus rate is between 0.995 and 0.9 
    - Repeat `Step 1` and `Step 2` on **ALL** non-palindromic variants to recalculate the consensus rate.
      -  If the recalculated rate is > 0.99, the palindromic variants are inferred to be aligned to the forward (or reverse) strand and harmonised accordingly. 
      - If the recalculated rate is ≤ 0.99, the palindromic variants are dropped from harmonisation to prevent errors.
  - If the consensus rate is ≤ 0.9
    - The palindromic variants are excluded from further harmonisation steps to ensure data integrity.
