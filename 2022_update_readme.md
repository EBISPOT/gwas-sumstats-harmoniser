# Compared with the original Snakefile, 10_percent_Snakefile includes four updates:
1. It allows inferring the direction of palindromic SNPs from 10% of total sites instead of running all sites (achieved by rule ten_percent_generate_strand_counts and rule ten_percent_summarise_strand_counts)
2. Considering the forward ratio can be affected by sampling error from 10% sampling, we set an interval that if the forward ratio > 0.995, the forward ratio from 10% sampling is accepted, while if the ratio is between 0.9 to 0.995, the original rules generate_strand_counts and summarise_strand_counts will run to get an accurate forward ration from all sites.
3. To improve the speed of the harmonization pipeline, there is a main_pysam.py replacing the original main.py to run faster.
4. Add a running log file: This file records the reference source, dbSNP version, and the direction of palindromic SNPs are inferred from 10% sites or from whole sites.

# Updated scripts:
1. Add 10_percent_Snakefile
2. Add sum_strand_counts_10percent.py (to support rerun selection to rerun whole sites)
3. Add main_pysam.py
4. Add packages in requirements.txt
