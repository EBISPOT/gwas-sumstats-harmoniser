---
slug: welcome
title: Welcome
authors: [yueji]
tags: [Sumstats]
---
👋 Welcome to our first release of the documentation for `gwas-sumstat-harmoniser`!
<!-- truncate -->

The harmonization pipeline, `gwas-sumstats-harmoniser`, was initially developed by the [Open Targets team](https://github.com/opentargets/genetics-sumstat-harmoniser) to: 
1. Determine the orientation of variants, 
2. Resolve RSIDs from locations and alleles, and 
3. Align the variants to the reference strand

Building on this foundation, the GWAS Catalog team developed our first pipeline for harmonizing GWAS summary statistic data, which includes mapping variants from various genome builds to GRCh38 and incorporating a QC step. They packaged the entire pipeline using Snakemake to enable automated data harmonization for all GWAS summary statistics in the GWAS Catalog.

Starting from 2022, the GWAS Catalog was looking for potentials of increasing the harmonisation efficiency

In 2023, the GWAS Catalog released the standard format on reporting the GWAS summary staitsic data, and we modified our pipeline and output corrospondingly.

With an increasing number of users interested in our pipeline, we have decided to create formal documentation to record as many details as possible about our pipeline, hoping it will benefit a wider audience. We are committed to keeping our GitHub repository publicly available. We welcome all questions and suggestions to improve both our pipeline and its documentation.







