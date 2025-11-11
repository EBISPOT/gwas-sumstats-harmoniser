#!/usr/bin/env python
# -*- coding: utf-8 -*-

# merge on hm_variant_id with vcf of desired build
# if variant_id is rsid and ID != variant_id or not synonym --> drop and create discrep df


import duckdb
from functools import lru_cache
import pandas as pd
import numpy as np

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


@lru_cache(maxsize=100000)
def cached_liftover(chrom, bp, build_map):
    """ Helper function for liftover """
    if not chrom or not bp: return None
    try:
        # pyliftover needs 'chr' prefix
        chrom_str = f"chr{chrom}" if not str(chrom).startswith('chr') else str(chrom)
        pos_int = int(bp)
        new_coords = build_map.convert_coordinate(chrom_str, pos_int)
        if new_coords and len(new_coords) > 0:
            return new_coords[0][1] # Return the new b38 position
    except Exception:
        return None
    return None

def process_standard_liftover_(ssdf, from_build, to_build, coordinate):
    """
    SCENARIO 1: (IF) File already has 'chromosome' and 'base_pair_location'.
    This is the original pipeline logic to perform a standard liftover.
    """
    print(f"File has '{CHR_DSET}'. Running standard liftover...")
    
    build_map = lft.LiftOver(lft.ucsc_release.get(from_build), lft.ucsc_release.get(to_build))

    ssdf[BP_DSET] = ssdf.apply(
        lambda row: cached_liftover(row[CHR_DSET], row[BP_DSET], build_map),
        axis=1
    )
    ssdf[HM_CC_DSET] = "lo" # Mark as 'liftover'
    return ssdf


# [REPLACE the old process_standard_liftover with this one]

def process_standard_liftover(ssdf, from_build, to_build, coordinate):
    """
    SCENARIO 1: (IF) File already has 'chromosome' and 'base_pair_location'.
    This is the original pipeline logic to perform a standard liftover.
    --- PATCHED to skip liftover if builds are the same ---
    """
    print(f"File has '{CHR_DSET}'. Running standard liftover...")

    # --- START OF NEW PATCH ---
    if from_build == to_build:
        print(f"from_build ({from_build}) and to_build ({to_build}) are the same. Skipping liftover.")
        ssdf[HM_CC_DSET] = "lo" # Mark as 'liftover' (even though skipped)
        return ssdf
    # --- END OF NEW PATCH ---

    build_map = lft.LiftOver(lft.ucsc_release.get(from_build), lft.ucsc_release.get(to_build))

    ssdf[BP_DSET] = ssdf.apply(
        lambda row: cached_liftover(row[CHR_DSET], row[BP_DSET], build_map),
        axis=1
    )
    ssdf[HM_CC_DSET] = "lo" # Mark as 'liftover'
    return ssdf

def process_hybrid_file(con, ssdf, vcf, from_build, to_build, coordinate):
    """
    SCENARIO 2 (ELSE): File has rsID / chr:pos strings.
    This is your custom logic to split, annotate, and liftover.
    """
    print(f"File has no '{CHR_DSET}'. Running custom rsID/chr:pos split logic...")
    
    # Split the data
    is_rsid_mask = ssdf[RSID].astype(str).str.startswith('rs')
    df_rsid = ssdf[is_rsid_mask].copy()
    df_non_rsid = ssdf[~is_rsid_mask].copy()
    print(f"Split into {len(df_rsid)} rsID rows and {len(df_non_rsid)} non-rsID rows.")
    
    # [ADD THESE TWO LINES]
    print(f"Headers of rsID dataframe: {df_rsid.columns.tolist()}")
    print(f"Headers of non-rsID dataframe: {df_non_rsid.columns.tolist()}")
    # [END OF NEW PRINT LINES]

    # --- 1. Process non-rsID (GRCh37) rows ---
    if not df_non_rsid.empty:
        print(f"Processing non-rsID rows (assumed GRCh37)...")
        try:
            # THIS IS YOUR NEW LOGIC: Parse chr:pos:ref:alt
            split_cols = df_non_rsid[RSID].str.split(':', expand=True)
            df_non_rsid[CHR_DSET] = split_cols[0]
            df_non_rsid[BP_DSET] = split_cols[1]
            # Keep the parsed alleles (A2=ref, A1=alt)
            df_non_rsid[OTHER_DSET] = split_cols[2]
            df_non_rsid[EFFECT_DSET] = split_cols[3]
        except Exception as e:
            print(f"WARNING: Could not split non-rsID column '{RSID}' into 4 parts. Setting coords to NA. Error: {e}")
            df_non_rsid[CHR_DSET] = 'NA'
            df_non_rsid[BP_DSET] = 'NA'
        
        # Initialize liftover from b37 (hg19) to b38 (hg38)
        build_map = lft.LiftOver(lft.ucsc_release.get(from_build), lft.ucsc_release.get(to_build))
        
        print(f"Performing liftover from b{from_build} to b{to_build}...")
        df_non_rsid[BP_DSET] = df_non_rsid.apply(
            lambda row: cached_liftover(row[CHR_DSET], row[BP_DSET], build_map),
            axis=1
        )
        df_non_rsid[HM_CC_DSET] = "lo" # Mark as 'liftover'
        print("Liftover complete.")

    # --- 2. Process rsID (GRCh38) rows ---
    if not df_rsid.empty:
        print("Processing rsID rows (annotating to GRCh38)...")
        con.register("df_rsid_view", df_rsid)
        
        # This is the reference VCF (parquet) glob pattern from your command
        vcfs = glob.glob(vcf) 
        files_sql = "[" + ", ".join(f"'{f}'" for f in vcfs) + "]"
        vcf_view = f"(SELECT * FROM read_parquet({files_sql}, union_by_name=true))"

        # Join to get GRCh38 coordinates
        query_rsid = f"""
        SELECT 
            ss.*, 
            vcf.CHR AS CHR_src, 
            vcf.POS AS POS_src,
            'rs' AS {HM_CC_DSET}
        FROM df_rsid_view ss
        LEFT JOIN {vcf_view} vcf ON ss.{RSID} = vcf.ID
        """
        df_rsid = con.execute(query_rsid).df()
        
        # Copy new GRCh38 coords, handling unmapped
        df_rsid[CHR_DSET] = df_rsid['CHR_src'].fillna(df_rsid[CHR_DSET])
        df_rsid[BP_DSET] = df_rsid['POS_src'].fillna(df_rsid[BP_DSET])
        # Also fill in alleles from reference if missing
        #df_rsid[EFFECT_DSET] = df_rsid[EFFECT_DSET].fillna(df_rsid['ALT_src'])
        #df_rsid[OTHER_DSET] = df_rsid[OTHER_DSET].fillna(df_rsid['REF_src'])
        df_rsid.drop(columns=['CHR_src', 'POS_src'], inplace=True, errors='ignore')
        print("Finished rsID annotation.")
    
    # --- 3. Re-combine ---
    return pd.concat([df_rsid, df_non_rsid], ignore_index=True)


def finalize_and_write_outputs(combined_df, chroms):
    """
    Final step for both scenarios:
    - Clean up coordinates
    - Write 'unmapped' file
    - Write per-chromosome '.merged' files for the next pipeline step
    """
    print("Writing output files...")
    
    # Clean up coordinates
    combined_df[CHR_DSET] = combined_df[CHR_DSET].astype("str").str.replace("\..*$","",regex=True)
    combined_df[BP_DSET] = combined_df[BP_DSET].astype("str").str.replace("\..*$","",regex=True)
    
    # Write variants missing CHR or BP to "unmapped"
    unmapped_df = combined_df[combined_df[CHR_DSET].isnull() | combined_df[BP_DSET].isnull() | (combined_df[BP_DSET] == 'None') | (combined_df[CHR_DSET] == 'None') | (combined_df[CHR_DSET] == 'NA')].copy()
    unmapped_outfile = os.path.join("unmapped")
    unmapped_df.to_csv(unmapped_outfile, sep="\t", index=False, na_rep="NA")
    
    # Write valid variants per chromosome
    valid_df = combined_df.dropna(subset=[CHR_DSET, BP_DSET])
    valid_df = valid_df[valid_df[BP_DSET] != 'None'] # Filter out failed liftovers
    valid_df = valid_df[valid_df[CHR_DSET] != 'NA']   # Filter out unmapped

    # Write per-chromosome files for the next pipeline step
    normalized_chroms = [normalize_chrom(c) for c in chroms]
    for chrom in normalized_chroms:
        chrom_str = str(chrom).split(".")[0]
        out_path = os.path.join("{}.merged".format(chrom_str))
        chrom_df = valid_df[valid_df[CHR_DSET] == chrom]
        
        if not chrom_df.empty:
            chrom_df.to_csv(out_path, sep="\t", index=False, na_rep="NA", mode='a')
        else:
            # create an empty file if no variants for this chromosome
            valid_df.head(0).to_csv(out_path, sep="\t", index=False)
    print("Finished writing per-chromosome .merged files.")


def merge_ss_vcf(ss, vcf, from_build, to_build, chroms, coordinate):
    """
    Main function (the "Router")
    - Loads and renames headers
    - Checks if file has coordinates
    - Calls the correct processing function (standard liftover or hybrid)
    - Writes the final output
    """
    
    con = duckdb.connect()

    # 1. Load the raw data
    #try:
    #    ssdf = con.execute(f"SELECT * FROM read_csv_auto('{ss}', SAMPLE_SIZE=10000)").df()
    #except Exception as e:
    
    # [THIS IS THE NEW, CORRECTED LINE]
    #try:
    #    ssdf = con.execute(f"SELECT * FROM read_csv_auto('{ss}', SAMPLE_SIZE=10000, types={{'CHR': 'VARCHAR', 'POS': 'VARCHAR'}})").df()
    #except Exception as e:
    #    print(f"CRITICAL ERROR: Could not read input file. Error: {e}")
    #    con.close()
    #    sys.exit(1)
    
    print("Reading file header to determine loading strategy...")
    try:
        # Read just the header to check for CHR/POS
        header = pd.read_csv(ss, sep='\t', nrows=0, comment='#').columns.tolist()
        header = [h.strip() for h in header] # Clean whitespace (e.g. ' P ' -> 'P')
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read file header. Error: {e}")
        con.close()
        sys.exit(1)

    # This is the 'if/else' statement you suggested
    if 'CHR' in header and 'POS' in header:
        print("Found CHR/POS. Loading with types={'CHR': 'VARCHAR', 'POS': 'VARCHAR'}")
        try:
            # This handles files with 'X' in the CHR column
            ssdf = con.execute(f"SELECT * FROM read_csv_auto('{ss}', SAMPLE_SIZE=10000, types={{'CHR': 'VARCHAR', 'POS': 'VARCHAR'}})").df()
        except Exception as e:
            print(f"CRITICAL ERROR: Could not read CHR/POS file. Error: {e}")
            con.close()
            sys.exit(1)
    else:
        print("Did not find CHR/POS. Loading as rsID-only file.")
        try:
            # This handles your rsID-only or rsID/hybrid files
            ssdf = con.execute(f"SELECT * FROM read_csv_auto('{ss}', sample_size=-1)").df()
        except Exception as e:
            print(f"CRITICAL ERROR: Could not read rsID-only file. Error: {e}")
            con.close()
            sys.exit(1)
    
    # --- START OF HEADER STRIPPING PATCH ---
    # Clean all column headers *after* loading
    ssdf.columns = ssdf.columns.str.strip()
    print(f"Loaded {len(ssdf)} rows. Headers cleaned.")
    # --- END OF HEADER STRIPPING PATCH ---

    # 2. Rename headers to standard (OUR UPGRADED PATCH)
    #header_rename_map = {
    #    # File 1 headers (MVP_R4...)
    #    'SNP_ID': RSID, 'alt': EFFECT_DSET, 'ref': OTHER_DSET, 'af': FREQ_DSET, 'log_or': OR_DSET, 'se_log_or': SE_DSET, 'pval': PVAL_DSET,
    #    # File 2 headers (gwas_bbj...)
    #    'rsid': RSID, 'A1': EFFECT_DSET, 'A2': OTHER_DSET, 'BETA': BETA_DSET, 'SE': SE_DSET, 'P': PVAL_DSET, 'N': 'n'
    #}
    
        # 2. Rename headers to standard (OUR UPGRADED PATCH)
    header_rename_map = {
        # --- File 1 headers (MVP_R4...) ---
        'SNP_ID': RSID,
        'alt': EFFECT_DSET,
        'ref': OTHER_DSET,
        'af': FREQ_DSET,
        'log_or': BETA_DSET,
        'se_log_or': SE_DSET,
        'pval': PVAL_DSET,
        
        # --- File 2 headers (gwas_bbj...) ---
        'rsid': RSID,
        'A1': EFFECT_DSET,
        'A2': OTHER_DSET,
        'BETA': BETA_DSET,
        'SE': SE_DSET,
        'P': PVAL_DSET,
        'N': 'n',

        # --- File 3 headers (GWASSummary_SakaueKanai...) ---
        'CHR': CHR_DSET,
        'POS': BP_DSET,
        'SNPID': RSID,
        'Allele2': EFFECT_DSET,    # Allele2 matches AF_Allele2
        'Allele1': OTHER_DSET,
        'AF_Allele2': FREQ_DSET,
        'p.value': PVAL_DSET
        # BETA and SE are already covered
    }

    columns_to_rename = {k: v for k, v in header_rename_map.items() if k in ssdf.columns}
    
    if columns_to_rename:
        print(f"Renaming columns: {columns_to_rename}")
        ssdf.rename(columns=columns_to_rename, inplace=True)
    
    # Add standard columns (e.g., chromosome) if they are missing
    add_fields_if_missing(df=ssdf) 

    # --- THIS IS THE IF/ELSE ROUTING LOGIC ---
    
    #if CHR_DSET in ssdf.columns and not ssdf[CHR_DSET].isnull().all():
    if CHR_DSET in columns_to_rename.values():
        # SCENARIO 1 (IF): File has an actual 'chromosome' column with data
        combined_df = process_standard_liftover(ssdf, from_build, to_build, coordinate)
    else:
        # SCENARIO 2 (ELSE): File has rsID / chr:pos strings. Run your custom logic.
        combined_df = process_hybrid_file(con, ssdf, vcf, from_build, to_build, coordinate)
    
    # --- END OF IF/ELSE ---

    con.close()
    
    # 7. Final step: Write outputs
    finalize_and_write_outputs(combined_df, chroms)

# Original code for this
def merge_ss_vcf_(ss, vcf, from_build, to_build, chroms, coordinate):

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
    
    # Adding this code for testing the annotation step
    con = duckdb.connect()
    try: 
        # Read header
        header = pd.read_csv(ss,sep='\t',nrows=0,comment='#').columns.tolist()
    except Exception as e:
        print(f"Error reading header of {ss}:{ee}")
        sys.exit(1) # exit with error if we cant read the file.
    
    if CHR_DSET in header:
        print(f"Found '{CHR_DSET}' column. Running standard query with chromosome filter ...")
    
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
        ssdf = con.execute(query).df()
    
    else:
        print(f"Did not find '{CHR_DSET}' column. Loading full file for rsID-based Annotations.")
        
        # This is the new, simpler query for rsID-only files
        query = f"""
        SELECT * FROM read_csv_auto('{ss}', SAMPLE_SIZE=10000)
        """
        ssdf = con.execute(query).df()
        
        # --- START OF NEW RENAMING PATCH ---
        # Define the mapping from your headers to the pipeline's standard headers
        header_rename_map = {
            'SNP_ID': RSID,
            'alt': EFFECT_DSET,
            'ref': OTHER_DSET,
            'af': FREQ_DSET,
            'log_or': OR_DSET,
            'se_log_or': SE_DSET,
            'pval': PVAL_DSET
        }
    
        # Create a final map of columns that actually exist in the file
        columns_to_rename = {
            k: v for k, v in header_rename_map.items() if k in ssdf.columns
        }
        
        if columns_to_rename:
            print(f"Renaming columns: {columns_to_rename}")
            ssdf.rename(columns=columns_to_rename, inplace=True)
        # --- END OF NEW RENAMING PATCH ---

    # handle the empty input file
    if ssdf.empty:
        print("No records in input summary statistics file.")
        return

    add_fields_if_missing(df=ssdf)
    rsid_mask = ssdf[RSID].str.startswith("rs").fillna(False)
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
            ON ssdf_rsid.{RSID} = vcf.ID
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
                                     ON ssdf_rsid.{RSID} = vcf.ID
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

def add_fields_if_missing_(df):
    add_column_to_df(df=df, column=RSID)
    add_column_to_df(df=df, column=CHR_DSET)
    add_column_to_df(df=df, column=BP_DSET)

def add_column_to_df_(df, column, value='NA'):
    if column not in df.columns:
        df[column] = value

# [THIS IS THE NEW, CORRECTED CODE]
def add_fields_if_missing(df):
    """
    Adds standard columns if they are missing.
    This patch ensures they are added with the correct data type (numeric/string).
    """
    # Add string columns with 'NA'
    add_column_to_df(df=df, column=RSID, is_numeric=False)
    add_column_to_df(df=df, column=EFFECT_DSET, is_numeric=False)
    add_column_to_df(df=df, column=OTHER_DSET, is_numeric=False)
    
    # Add numeric columns with np.nan
    add_column_to_df(df=df, column=CHR_DSET, is_numeric=True)
    add_column_to_df(df=df, column=BP_DSET, is_numeric=True)
    add_column_to_df(df=df, column=BETA_DSET, is_numeric=True)
    add_column_to_df(df=df, column=OR_DSET, is_numeric=True)
    add_column_to_df(df=df, column=SE_DSET, is_numeric=True)
    add_column_to_df(df=df, column=PVAL_DSET, is_numeric=True)
    add_column_to_df(df=df, column=FREQ_DSET, is_numeric=True)


def add_column_to_df(df, column, is_numeric=False):
    """
    Helper function to add a column with the correct null type.
    """
    if column not in df.columns:
        if is_numeric:
            df[column] = np.nan # Use a numeric (float) null
        else:
            df[column] = "NA"   # Use the default string 'NA'

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