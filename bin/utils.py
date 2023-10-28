#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import yaml
import pandas as pd
import argparse

sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


def get_chromosome_list(config_path):
    return get_properties(config_path)['chromosomes']

def get_properties(config_path):
    with open(config_path, 'r') as stream:
        configs = yaml.load(stream, Loader=yaml.FullLoader)
        return configs

def get_csv_reader(csv_file):
    dialect = csv.Sniffer().sniff(csv_file.readline())
    csv_file.seek(0)
    return csv.reader(csv_file, dialect)

def get_filename(file):
    return file.split("/")[-1].split(".")[0]

def list_headers(csv_file):
    header_list = pd.read_table(csv_file, nrows=1).columns.values.tolist()
    return header_list

def get_harmonisation_mapper_args(csv_file):
    HARMONISER_ARG_MAP = {
    CHR_DSET: "--chrom_col",
    BP_DSET: "--pos_col",
    EFFECT_DSET: "--effAl_col",
    OTHER_DSET: "--otherAl_col",
    RSID: "--rsid_col",
    BETA_DSET: "--beta_col",
    OR_DSET: "--or_col",
    RANGE_L_DSET: "--or_col_lower",
    RANGE_U_DSET: "--or_col_upper",
    FREQ_DSET: "--eaf_col",
    HM_CC_DSET:"--hm_coordinate_conversion"
}
    arg_list = ["{v} {k}".format(k=k, v=v) for k, v in HARMONISER_ARG_MAP.items() if k in list_headers(csv_file)]
    harm_args = " ".join(arg_list)
    return harm_args

def get_strandcount_mapper_args(csv_file):
    STRAND_COUNT_ARG_MAP = {
    CHR_DSET: "--chrom_col",
    BP_DSET: "--pos_col",
    EFFECT_DSET: "--effAl_col",
    OTHER_DSET: "--otherAl_col",
    RSID: "--rsid_col",
    HM_CC_DSET:"--hm_coordinate_conversion"
}
    arg_list = ["{v} {k}".format(k=k, v=v) for k, v in STRAND_COUNT_ARG_MAP.items() if k in list_headers(csv_file)]
    count_args = " ".join(arg_list)
    return count_args

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-harm_args',
                           help='Return the header mapper arguments for the harmonisation',
                           action='store_true')
    argparser.add_argument('-strand_count_args',
                           help='Return the header mapper arguments for strand count step',
                           action='store_true')
    argparser.add_argument('-input_version', help='input_version', nargs='?', 
                           const="GWAS-SSFv1.0", required=True)
    args = argparser.parse_args()

    in_version=args.input_version

    if in_version=="pre-GWAS-SSF":
        global RSID
        RSID="variant_id"

    ss_file = args.f
    harm_args = args.harm_args
    count_args=args.strand_count_args

    if harm_args:
        print(get_harmonisation_mapper_args(ss_file))
    if count_args:
        print(get_strandcount_mapper_args(ss_file))


if __name__ == '__main__':
    main()