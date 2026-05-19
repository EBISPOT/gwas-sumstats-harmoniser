---
sidebar_position: 1
---
# Codes in Harmonised Sumstats
`hm_coordinate_conversion`
| Value | Description of the genome mapping process                   |
|------|-------------------------------------------------------------------------|
| rs    | Update base pair location value by mapping rsID using reference           |
| lo    | liftover base pair location to the target genome build (GRCh 38)              |

`hm_code`
| Code | Description of Harmonisation Process                                      |
|------|-------------------------------------------------------------------------|
| 1    | Palindromic; Infer strand; Forward strand; Alleles correct              |
| 2    | Palindromic; Infer strand; Forward strand; Flipped alleles              |
| 3    | Palindromic; Infer strand; Reverse strand; Alleles correct              |
| 4    | Palindromic; Infer strand; Reverse strand; Flipped alleles              |
| 5    | Palindromic; Assume forward strand; Alleles correct                     |
| 6    | Palindromic; Assume forward strand; Flipped alleles                     |
| 7    | Palindromic; Assume reverse strand; Alleles correct                     |
| 8    | Palindromic; Assume reverse strand; Flipped alleles                     |
| 9    | Palindromic; Drop palindromic; Not harmonised                           |
| 10   | Forward strand; Alleles correct                                         |
| 11   | Forward strand; Flipped alleles                                         |
| 12   | Reverse strand; Alleles correct                                         |
| 13   | Reverse strand; Flipped alleles                                         |
| 14   | Required fields are not known; Not harmonised                          |
| 15   | No matching variants in reference VCF; Not harmonised                   |
| 16   | Multiple matching variants in reference VCF; Not harmonised             |
| 17   | Palindromic; Infer strand; EAF or reference VCF AF not known; Not harmonised |
| 18   | Palindromic; Infer strand; EAF < specified minor allele frequency threshold; Not harmonised |
