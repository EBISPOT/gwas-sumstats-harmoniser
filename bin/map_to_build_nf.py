#!/usr/bin/env python
# -*- coding: utf-8 -*-

# merge on hm_variant_id with vcf of desired build
# if variant_id is rsid and ID != variant_id or not synonym --> drop and create discrep df


import duckdb
from functools import lru_cache
import pandas as pd

import liftover as lft
from common_constants import *
import os
import glob
import argparse
from ast import literal_eval

# Allow very large fields in input file-------------
import sys
import csv

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

# map_to_build----------------------------------------------------
# process the chr: if it is 23,24,25, convert to X,Y,MT
def normalize_chrom(c):
    return {"23": "X", "24": "Y", "25": "MT"}.get(str(c).upper(), str(c).upper())

def merge_ss_vcf(ss, vcf, from_build, to_build, chroms, coordinate):

    """
    Merge GWAS summary stats with reference VCFs by RSID, liftover unmapped variants,
    and write per-chromosome outputs.

    Parameters:
    - ss (str): Path to summary statistics file
    - vcf (str): Glob pattern for reference VCF Parquet files
    - from_build (str): Genome build of the summary stats
    - to_build (str): Target genome build for output
    - chroms (list[str]): List of chromosomes to write output for
    - coordinate: (Unused)
    """

    vcfs = glob.glob(vcf)
    # read input sumstats file using the duckdb and convert the 23,24,25 into X,Y,MT in the chromsome column
    normalized_chroms = [normalize_chrom(c) for c in chroms]
    chrom_filter = ",".join(f"'{c}'" for c in normalized_chroms)
    
    query = f"""
    SELECT *
    FROM (
    SELECT
      CASE
        WHEN CAST({CHR_DSET} AS VARCHAR) = '23' THEN 'X'
        WHEN CAST({CHR_DSET} AS VARCHAR) = '24' THEN 'Y'
        WHEN CAST({CHR_DSET} AS VARCHAR) = '25' THEN 'MT'
        ELSE CAST({CHR_DSET} AS VARCHAR)
      END AS {CHR_DSET},
      *
    EXCLUDE {CHR_DSET}
    FROM read_csv_auto('{ss}', SAMPLE_SIZE=10000)
    ) mapped
    WHERE {CHR_DSET} IN ({chrom_filter})
    """
    con = duckdb.connect()
    ssdf = con.execute(query).df()

    # handle the empty input file
    if ssdf.empty:
        print("No records in input summary statistics file.")
        return

    add_fields_if_missing(df=ssdf)
    rsid_mask = ssdf[SNP_DSET].str.startswith("rs").fillna(False)
    ssdf_with_rsid = ssdf[rsid_mask].copy()
    ssdf_without_rsid = ssdf[~rsid_mask].copy()
    header = list(ssdf.columns.values)

    print("starting rsid mapping")
    print("ssdf with rsid empty?: {}".format(ssdf_with_rsid.empty))

    # if there are records with rsids
    # Initialize an empty DataFrame for merged results 
    merged_vcf = pd.DataFrame()
    if not ssdf_with_rsid.empty:
        # registers a data frame as a virtual table (view) in a DuckDB connection
        con.register("ssdf_rsid", ssdf_with_rsid)

        # Read the reference VCF files and create a union of them
        files_sql = "[" + ", ".join(f"'{f}'" for f in vcfs) + "]"
        vcf_view = f"(SELECT * FROM read_parquet({files_sql}, union_by_name=true))"


         # Step 3: Join once across all VCFs
        merged_vcf = con.execute(f"""
            SELECT 
                ssdf_rsid.*, 
                vcf.CHR AS CHR_src, 
                vcf.POS AS POS_src, 
                'rs' AS {HM_CC_DSET}
            FROM ssdf_rsid
            LEFT JOIN {vcf_view} vcf
            ON ssdf_rsid.{SNP_DSET} = vcf.ID
            WHERE vcf.ID IS NOT NULL
            """).df()
        
         # Step 4: Format CHR/POS and save
        merged_vcf[CHR_DSET] = merged_vcf["CHR_src"].astype("str").str.replace(r"\..*$", "", regex=True)
        merged_vcf[BP_DSET] = merged_vcf["POS_src"].astype("str").str.replace(r"\..*$", "", regex=True)
        merged_vcf = merged_vcf[header + [HM_CC_DSET]]  # Add back only needed cols
        
        # Step 6: keep the unmapped variants
        # Unmapped rsIDs
        ssdf_with_rsid = con.execute(f"""
                                     SELECT ssdf_rsid.*
                                     FROM ssdf_rsid
                                     LEFT JOIN {vcf_view} vcf
                                     ON ssdf_rsid.{SNP_DSET} = vcf.ID
                                     WHERE vcf.ID IS NULL
                                     """).df()
        con.close()

    print("finished rsid mapping")
    # liftover the snps without rsids and those with unrecognised rsids 
    print("liftover remaining variants")
    ssdf = pd.concat([ssdf_with_rsid, ssdf_without_rsid])
    ssdf[HM_CC_DSET] = "lo"

    build_map = None
    if from_build != to_build:
        build_map = lft.LiftOver(lft.ucsc_release.get(from_build), lft.ucsc_release.get(to_build))
        
    @lru_cache(maxsize=100000)
    def cached_liftover(chrom, bp):
        try:
            return lft.map_bp_to_build_via_liftover(chromosome=chrom, bp=bp, build_map=build_map, coordinate=coordinate[0])
        except:
            return None
    # liftover the unmapped variants by chr and position
    if build_map:
        ssdf[BP_DSET] = [
            cached_liftover(str(chrom), str(bp)) if pd.notnull(chrom) and pd.notnull(bp) else None
            for chrom, bp in zip(ssdf[CHR_DSET], ssdf[BP_DSET])
            ]
    print("liftover complete")
    # merge "rs" and "lo" result to write the output
    combined_df = pd.concat([merged_vcf, ssdf], ignore_index=True)
    combined_df[CHR_DSET] = combined_df[CHR_DSET].astype("str").str.replace("\..*$","",regex=True)
    combined_df[BP_DSET] = combined_df[BP_DSET].astype("str").str.replace("\..*$","",regex=True)
    
    # 1. Write variants missing CHR or BP to "unmapped"
    unmapped_df = combined_df[combined_df[CHR_DSET].isnull() | combined_df[BP_DSET].isnull()].copy()
    unmapped_outfile = os.path.join("unmapped")
    unmapped_df.to_csv(unmapped_outfile, sep="\t", index=False, na_rep="NA")
    
    # 2. Write valid variants per chromosome
    valid_df = combined_df.dropna(subset=[CHR_DSET, BP_DSET])
    """ Only chr with variants in input file are written
    chrom_set = set(normalized_chroms)
    for chrom, group_df in valid_df.groupby(CHR_DSET, sort=False):
        if chrom in chrom_set:
            chrom_str = str(chrom).split(".")[0]
            out_path = os.path.join("{}.merged".format(chrom_str))
            group_df.to_csv(out_path, sep="\t", index=False, na_rep="NA", mode='a')
    """
    # Write output files for each chroms (nextflow check the number of output == nchr hope to run)
    for chrom in normalized_chroms:
        chrom_str = str(chrom).split(".")[0]
        out_path = os.path.join("{}.merged".format(chrom_str))
        chrom_df = valid_df[valid_df[CHR_DSET] == chrom]
        if not chrom_df.empty:
            chrom_df.to_csv(out_path, sep="\t", index=False, na_rep="NA", mode='a')
        else:
            # create an empty file if no variants for this chromosome
            valid_df.head(0).to_csv(out_path, sep="\t", index=False)

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
    add_column_to_df(df=df, column=SNP_DSET)
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