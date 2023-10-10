#!/usr/bin/env python
# -*- coding: utf-8 -*-

# merge on hm_variant_id with vcf of desired build
# if variant_id is rsid and ID != variant_id or not synonym --> drop and create discrep df


import pandas as pd
import liftover as lft
from common_constants import *
import os
import glob
import argparse
from ast import literal_eval


def merge_ss_vcf(ss, vcf, from_build, to_build, chroms, coordinate):
    vcfs = glob.glob(vcf)
    ssdf = pd.read_table(ss, sep=None, engine='python', dtype=str)
    add_fields_if_missing(df=ssdf)
    rsid_mask = ssdf[RSID].str.startswith("rs").fillna(False)
    ssdf_with_rsid = ssdf[rsid_mask]
    ssdf_without_rsid = ssdf[~rsid_mask]
    header = list(ssdf.columns.values)
    print("starting rsid mapping")
    print("ssdf with rsid empty?: {}".format(ssdf_with_rsid.empty))
    # if there are records with rsids
    if not ssdf_with_rsid.empty:
        for vcf in vcfs:
            vcf_df = pd.read_parquet(vcf)
            chrom = vcf_df.CHR[0]
            # merge on rsid, update chr and position from vcf ref
            mergedf = ssdf_with_rsid.merge(vcf_df, left_on=RSID, right_on="ID", how="left")
            mapped = mergedf.dropna(subset=["ID"]).drop([CHR_DSET, BP_DSET], axis=1)
            mapped[CHR_DSET] = mapped["CHR"].astype("str").str.replace("\..*$","")
            mapped[BP_DSET] = mapped["POS"].astype("str").str.replace("\..*$","")
            mapped = mapped[header]
            mapped[HM_CC_DSET] = "rs"
            outfile = os.path.join("{}.merged".format(chrom))
            mapped.to_csv(outfile, sep="\t", index=False, na_rep="NA")
            ssdf_with_rsid = mergedf[mergedf["ID"].isnull()]
            ssdf_with_rsid = ssdf_with_rsid[header]
            if ssdf_with_rsid.empty:
                break
    
    print("finished rsid mapping")
    # liftover the snps without rsids and those with unrecognised rsids 
    print("liftover remaining variants")
    ssdf = pd.concat([ssdf_with_rsid, ssdf_without_rsid])
    ssdf[HM_CC_DSET] = "lo"
    build_map = lft.LiftOver(lft.ucsc_release.get(from_build), lft.ucsc_release.get(to_build)) if from_build != to_build else None
    if build_map:
        ssdf[BP_DSET] = [lft.map_bp_to_build_via_liftover(chromosome=x, bp=str(int(y)), build_map=build_map,coordinate=coordinate[0]) for x, y in zip(ssdf[CHR_DSET], ssdf[BP_DSET])]
    for chrom in chroms:
        print(chrom)
        df = ssdf.loc[ssdf[CHR_DSET].astype("str") == chrom]
        df = df.dropna(subset=[BP_DSET])
        df[BP_DSET] = df[BP_DSET].astype("str").str.replace("\..*$","")
        outfile = os.path.join("{}.merged".format(chrom))
        if os.path.isfile(outfile):
            print("df to {}".format(outfile))
            print(df)
            df.to_csv(outfile, sep="\t", mode='a', header=False, index=False, na_rep="NA")
        else:
            print("df to {}".format(outfile))
            print(df)
            df.to_csv(outfile, sep="\t", mode='w', index=False, na_rep="NA")
    print("liftover complete")
    no_chr_df = ssdf[ssdf[CHR_DSET].isnull()]
    no_pos_df = ssdf[ssdf[BP_DSET].isnull()]
    no_loc_df = pd.concat([no_chr_df, no_pos_df])
    no_loc_df[HM_CC_DSET] = "NA"
    outfile = os.path.join("unmapped")
    no_loc_df.to_csv(outfile, sep="\t", index=False, na_rep="NA")

def listify_string(string):
    """
    listify the input. If it's a list leave it.
    It it looks like at list, make it a list.
    Otherwise convert the input into a list - which is probably not what's wanted.
    :param string:
    :return: a list
    """
    if type(string) is str:
        if "[" and "]" in string:
            new=string.replace(" ", "").replace("[", '["').replace("]", '"]').replace(",", '","')
            listified = literal_eval(new)
        else:
            listified = list(string)
    elif type(string) is list:
        listified = string
    else:
        listified = list(str(string))
    return listified

def add_fields_if_missing(df):
    add_column_to_df(df=df, column=RSID)
    add_column_to_df(df=df, column=CHR_DSET)
    add_column_to_df(df=df, column=BP_DSET)

def add_column_to_df(df, column, value='NA'):
    if column not in df.columns:
        df[column] = value



def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-vcf', help='The name of the vcf file', required=True)
    argparser.add_argument('--log', help='The name of the log file')
    argparser.add_argument('-from_build', help='The original build e.g. "36" for NCBI36 or hg18', required=True)
    argparser.add_argument('-to_build', help='The latest (desired) build e.g. "38"', required=True)
    argparser.add_argument('-chroms', help='A list of chromosomes to process', default=DEFAULT_CHROMS)
    argparser.add_argument('-coordinate', help='index', nargs='?', const="1-based", required=True)
    args = argparser.parse_args()

    ss = args.f
    vcf = args.vcf
    from_build = args.from_build
    to_build = args.to_build
    chroms = listify_string(args.chroms)
    coordinate=args.coordinate


    merge_ss_vcf(ss, vcf, from_build, to_build, chroms, coordinate)



if __name__ == "__main__":
    main()
