#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import argparse
import logging
import sqlite3

from ensembl_rest_client import EnsemblRestClient
from common_constants import *

csv.field_size_limit(sys.maxsize)


logger = logging.getLogger('basic_qc')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')

# 1) must have SNP, PVAL, CHR, BP
# 2) *coerce headers
# 3) *if variant_id is None: try to set it to hm_variant_id or set variant_id to hm_variant_id?
# ---- The consequence of that is that we are both filling in and also removing if the rsids don't map any more
# ---- Do we want to keep rsids even if they don't map - and certainly don't map to the coordinates
# ---- OR if hm_rsid is not None, set variant_id = hm_rsid, thus updating all rsids where possible and keeping
# ---- ones that where not harmonised. 
# 3) remove rows with blank values for (1)
# 4) conform to data types:
#   - if pval not floats: remove row
#   - if chr and bp not ints: remove row
# 5) set chr 'x' and 'y' to 23 and 24

hm_header_transformations = {

    # variant id
    'hm_varid': HM_VAR_ID,
    # odds ratio
    'hm_OR': HM_OR_DSET,
    # or range
    'hm_OR_lowerCI': HM_RANGE_L_DSET, 
    'hm_OR_upperCI': HM_RANGE_U_DSET,
    # beta
    'hm_beta': HM_BETA_DSET,
    # effect allele
    'hm_effect_allele': HM_EFFECT_DSET,
    # other allele
    'hm_other_allele': HM_OTHER_DSET,
    # effect allele frequency
    'hm_eaf': HM_FREQ_DSET,
    # harmonisation code
    'hm_code': HM_CODE
}


REQUIRED_HEADERS = [RSID, PVAL_DSET, CHR_DSET, BP_DSET]
BLANK_SET = {'', ' ', '-', '.', 'na', None, 'none', 'nan', 'nil'}

# hm codes to drop
HM_CODE_FILTER = {9,14,15,16,17,18}


def refactor_header(header):
    return [hm_header_transformations[h] if h in hm_header_transformations else h for h in header]


def check_for_required_headers(header):
    return list(set(REQUIRED_HEADERS) - set(header))


def get_header_indices(header):
    return [header.index(h) for h in REQUIRED_HEADERS if h in header] 


def required_elements(row, header):
    return [row[i] for i in get_header_indices(header)]
      

def lowercase_list(lst):
    return [x.lower() for x in lst]


def remove_row_if_required_is_blank(row, header):
    blanks = BLANK_SET & set(required_elements(lowercase_list(row), header))
    if blanks:
        return True
    else:
        return False

def remove_row_if_unharmonisable(row, header):
    if row[header.index(HM_CODE)] in HM_CODE_FILTER:
        return True
    else:
        return False

def blanks_to_NA(row):
    for n, i in enumerate(row):
        if i.lower() in BLANK_SET:
            row[n] = 'NA'
    return row
            

def remove_row_if_wrong_data_type(row, header, col, data_type):
    try:
        data_type(row[header.index(col)])
        return False
    except ValueError:
        return True


def map_chr_values_to_numbers(row, header):
    index_chr = header.index(CHR_DSET)
    chromosome = row[index_chr].lower()
    if 'x' in chromosome:
        chromosome = '23'
    if 'y' in chromosome:
        chromosome = '24'
    if 'mt' in chromosome:
        chromosome = '25'
    row[index_chr] = chromosome
    return row



def drop_last_element_from_filename(filename):
    filename_parts = filename.split('-')
    return '-'.join(filename_parts[:-1])

def get_csv_reader(csv_file):
    dialect = csv.Sniffer().sniff(csv_file.readline())
    csv_file.seek(0)
    return csv.reader(csv_file, dialect)


def get_filename(file):
    return file.split("/")[-1].split(".")[0]


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-o', help='The name of the output file', required=True)
    argparser.add_argument('-db', help='The name of the synonyms database. If not provided ensembl rest API will be used', default=None, required=False)
    argparser.add_argument('--print_only', help='only print the lines removed and do not write a new file', action='store_true')
    argparser.add_argument('--log', help='The name of the log file')
    args = argparser.parse_args()

    file = args.f
    db = args.db
    filename=args.o
    log_file = args.log

    new_filename=filename

    header = None
    is_header = True

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    with open(file) as csv_file:
        writer = None
        if not args.print_only:
            result_file = open(new_filename, 'w')
            writer = csv.writer(result_file, delimiter='\t')
        csv_reader = get_csv_reader(csv_file)

        for index, row in enumerate(csv_reader):
            if is_header:
                header = refactor_header(row)
                is_header = False
                missing_headers = check_for_required_headers(header)
                if missing_headers:
                    sys.exit("Required headers are missing!:{}".format(missing_headers))
                if not args.print_only:
                    writer.writerows([header])
            else:
                # First try to replace an invalid variant_id with the hm_rsid
                # Checks for blanks, integers and floats:
                row = blanks_to_NA(row)
                row = map_chr_values_to_numbers(row, header)
                unharmonisable = remove_row_if_unharmonisable(row, header)
                blank = remove_row_if_required_is_blank(row, header)
                wrong_type_chr = (remove_row_if_wrong_data_type(row, header, CHR_DSET, int)) 
                wrong_type_bp = (remove_row_if_wrong_data_type(row, header, BP_DSET, int))
                wrong_type_pval = (remove_row_if_wrong_data_type(row, header, PVAL_DSET, float))
                remove_row_tests = [unharmonisable == False,
                                    blank == False,
                                    wrong_type_chr == False,
                                    wrong_type_bp == False,
                                    wrong_type_pval == False]
                if all(remove_row_tests):
                    if not args.print_only:
                        writer.writerows([row])
                else:
                    # print lines that are removed
                    hm_code = int(row[header.index(HM_CODE)])
                    if hm_code not in HM_CODE_FILTER:
                        hm_code = 19
                    logger.info(f'Removing record number {index}, with hm_code {hm_code}')

           
if __name__ == "__main__":
    main()
