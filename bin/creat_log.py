#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import argparse
import sys

data = sys.stdin.readlines()
file = pd.DataFrame(data, columns=['hm_code'])
# file=pd.read_csv("/Users/yueji/downloads/del.tsv",sep='\t')
file["count"]=1
result=file.groupby(["hm_code"])[["count"]].sum()
all=sum(result["count"].tolist())
result["percentage"]=result["count"]/all

code_table = {
        1:  'Palindromic; Infer strand; Forward strand; Correct orientation; Already harmonised',
        2:  'Palindromic; Infer strand; Forward strand; Flipped orientation; Requires harmonisation',
        3:  'Palindromic; Infer strand; Reverse strand; Correct orientation; Already harmonised',
        4:  'Palindromic; Infer strand; Reverse strand; Flipped orientation; Requires harmonisation',
        5:  'Palindromic; Assume forward strand; Correct orientation; Already harmonised',
        6:  'Palindromic; Assume forward strand; Flipped orientation; Requires harmonisation',
        7:  'Palindromic; Assume reverse strand; Correct orientation; Already harmonised',
        8:  'Palindromic; Assume reverse strand; Flipped orientation; Requires harmonisation',
        9:  'Palindromic; Drop palindromic; Will not harmonise',
        10: 'Forward strand; Correct orientation; Already harmonised',
        11: 'Forward strand; Flipped orientation; Requires harmonisation',
        12: 'Reverse strand; Correct orientation; Already harmonised',
        13: 'Reverse strand; Flipped orientation; Requires harmonisation',
        14: 'Required fields are not known; Cannot harmonise',
        15: 'No matching variants in reference VCF; Cannot harmonise',
        16: 'Multiple matching variants in reference VCF (ambiguous); Cannot harmonise',
        17: 'Palindromic; Infer strand; EAF or reference VCF AF not known; Cannot harmonise',
        18: 'Palindromic; Infer strand; EAF < --maf_palin_threshold; Will not harmonise' }

result["hm_code"]=result.index.astype(int,copy=False)

# successfully harmonized variants
print("\n5. Successfully harmonised variants:\n")
success=result[(result['hm_code'] <14) & (result['hm_code'] !=9)]
success_all=sum(success["count"].tolist())
success_ratio=success_all/all
print ("{0:.2%}".format(success_ratio),"(",success_all, "of", all ,") sites successfully harmonised.\n")
print("hm_code","Number","Percentage","Explanation",sep="\t")
for i in range(0,len(success.index)):
    key=success.iloc[i,2]
    count=success.iloc[i,0]
    per=success.iloc[i,1]
    print(key,count,"{0:.2%}".format(per),code_table[key],sep="\t")

# Failed harmonized variants
print("\n6. Failed harmonisation:\n")
fail=result[(result['hm_code'] ==9) | (result['hm_code'] >13)]
fail_all=sum(fail["count"].tolist())
fail_ratio=fail_all/all
print ("{0:.2%}".format(fail_ratio),"(",fail_all, "of", all ,") sites failed to harmonise.\n")

print("hm_code","Number","Percentage","Explanation",sep="\t")
for i in range(0,len(fail.index)):
    key=fail.iloc[i,2]
    count=fail.iloc[i,0]
    per=fail.iloc[i,1]
    print(key,count,"{0:.2%}".format(per),code_table[key],sep="\t")

print("\n7. Overview")
if fail_ratio==1.0:
    print ("Result","FAILED_HARMONIZATION",sep="\t")
else:
    print ("Result","SUCCESS_HARMONIZATION",sep="\t")
