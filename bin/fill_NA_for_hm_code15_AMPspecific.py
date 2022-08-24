#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import glob
import argparse
import numpy as np
from copy import deepcopy
from subprocess import Popen, PIPE
from collections import OrderedDict, Counter

def complement(s): 
    basecomplement = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A', 'N': 'N'} 
    letters = list(s) 
    letters = [basecomplement[base] for base in letters] 
    return ''.join(letters)
def revcom(s):
    return complement(s[::-1])


def fill_hm_15(ss,palin_mode,outfile):
    ssdf = pd.read_csv(ss, sep='\t', dtype=str)
    print("read done")
    # function 1: test the hm_code and whether this site is an indel （length） 
    # ssdf["hm_code"]=="15"
    # ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3
    
    # function 2: fill required column with the raw data (direction)
    ssdf["hm_rsid"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["variant_id"],ssdf["hm_rsid"])
    print("rsid done")
    ssdf["hm_chrom"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["chromosome"],ssdf["hm_chrom"])
    print("chrom done")
    ssdf["hm_pos"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["base_pair_location"],ssdf["hm_pos"])
    print("pos done")
    ssdf["hm_beta"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["beta"],ssdf["hm_beta"])
    print("beta done")
    ssdf["hm_odds_ratio"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["odds_ratio"],ssdf["hm_odds_ratio"])
    print("or done")
    
    if palin_mode == 'forward':
        ssdf["hm_other_allele"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["other_allele"],ssdf["hm_other_allele"])
        ssdf["hm_effect_allele"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf["effect_allele"],ssdf["hm_effect_allele"])
        ssdf["hm_code"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),16,ssdf["hm_code"])
        # Add new code hm_code=16, which changed from 15, means alleles in the data do not match reference alleles, but they are indels, so keep all raw data ifnormation here.
    elif palin_mode == 'reverse':
        print("reverse_mode")
        ssdf["hm_other_allele"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf['other_allele'].apply(revcom),ssdf["hm_other_allele"])
        print("re-other done")
        ssdf["hm_effect_allele"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),ssdf['effect_allele'].apply(revcom),ssdf["hm_effect_allele"])
        print("re-effect done")
        ssdf["hm_code"]=np.where((ssdf["hm_code"]=="15")&(ssdf["effect_allele"].str.len()+ssdf["other_allele"].str.len()>=3),17,ssdf["hm_code"])
        print("code done")
        # Add new code hm_code=17, which changed from 15, means alleles in the data do not match reference alleles, but they are indels, so keep revcomp allele of raw data at here.
        
    ssdf.to_csv(outfile, sep="\t", index=False, na_rep="NA")


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-p', help='The major direction of the data', required=True)
    argparser.add_argument('-o', help='output name', required=True)
    args = argparser.parse_args()

    ss = args.f
    palin_mode = args.p
    outfile=args.o
    
    fill_hm_15(ss,palin_mode,outfile)

    
if __name__ == "__main__":
    main()

