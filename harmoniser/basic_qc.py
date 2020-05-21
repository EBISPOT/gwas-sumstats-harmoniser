import csv
import sys
import argparse
import logging
import sqlite3

from formatting_tools.sumstats_formatting import *
from formatting_tools.ensembl_rest_client import EnsemblRestClient
sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *

csv.field_size_limit(sys.maxsize)


logger = logging.getLogger('basic_qc')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')

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



class sqlClient():
    def __init__(self, database):
        self.database = database
        self.conn = self.create_conn()
        self.cur = self.conn.cursor()

    def create_conn(self):
        try:
            conn = sqlite3.connect(self.database)
            conn.row_factory = sqlite3.Row
            return conn
        except NameError as e:
            print(e)
        return None

    def get_synonyms(self, rsid):
        data = []
        for row in self.cur.execute("select name from variation_synonym where variation_id in (select variation_id from variation_synonym where name =?)", (rsid,)):
            data.append(row[0])
        return data



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


REQUIRED_HEADERS = [SNP_DSET, PVAL_DSET, CHR_DSET, BP_DSET]
BLANK_SET = {'', ' ', '-', '.', 'na', None, 'none', 'nan', 'nil'}


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


def resolve_invalid_rsids(row, header, ensembl_client=None, sql_client=None):
    hm_rsid_idx = header.index('hm_rsid')
    snp_idx = header.index(SNP_DSET)
    # if possible, set variant_id to harmonised rsid
    if row[hm_rsid_idx].startswith('rs'):
        # check that if rsID already present is not synonym of that found in vcf
        if row[snp_idx].startswith('rs') and row[snp_idx] != row[hm_rsid_idx]:
            synonyms = []
            if ensembl_client:
                print("ens")
                rs_info = ensembl_client.get_rsid(row[snp_idx])
                if rs_info != "NA":
                    try:
                        synonyms = rs_info["synonyms"]
                        synonyms.append(rs_info["name"])
                    except TypeError:
                        row[snp_idx] = 'NA'
            elif sql_client:
                print("sql")
                synonyms = sql_client.get_synonyms(row[snp_idx])
            print(synonyms)
            if row[hm_rsid_idx] in synonyms:
                row[snp_idx] = row[hm_rsid_idx]
            else:
                row[snp_idx] = 'NA'
        else:
            row[snp_idx] = row[hm_rsid_idx]
    # if variant_id is doesn't begin 'rs' 
    if not row[snp_idx].startswith('rs'):
        row[snp_idx] = 'NA'
    return row


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-d', help='The name of the output directory', required=True)
    argparser.add_argument('-db', help='The name of the synonyms database. If not provided ensembl rest API will be used', default=None, required=False)
    argparser.add_argument('--print_only', help='only print the lines removed and do not write a new file', action='store_true')
    argparser.add_argument('--log', help='The name of the log file')
    args = argparser.parse_args()

    file = args.f
    db = args.db
    out_dir = args.d
    log_file = args.log
    filename = get_filename(file)

    #new_filename = out_dir + drop_last_element_from_filename(filename) + '.tsv' # drop the build from the filename
    new_filename = out_dir + filename + '.qc.tsv' 
    
    header = None
    is_header = True
    lines = []
    removed_lines =[]
    missing_headers = []

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    with open(file) as csv_file:
        result_file = None
        writer = None
        if not args.print_only:
            result_file = open(new_filename, 'w')
            writer = csv.writer(result_file, delimiter='\t')
        csv_reader = get_csv_reader(csv_file)

        for row in csv_reader:
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
                sql_client = sqlClient(db) if db else None
                ensembl_client = EnsemblRestClient() if not db else None
                row = resolve_invalid_rsids(row, header, ensembl_client, sql_client)
                row = blanks_to_NA(row)
                row = map_chr_values_to_numbers(row, header)
                blank = remove_row_if_required_is_blank(row, header)
                wrong_type_chr = (remove_row_if_wrong_data_type(row, header, CHR_DSET, int)) 
                wrong_type_bp = (remove_row_if_wrong_data_type(row, header, BP_DSET, int))
                wrong_type_pval = (remove_row_if_wrong_data_type(row, header, PVAL_DSET, float))
                remove_row_tests = [blank == False,
                                    wrong_type_chr == False,
                                    wrong_type_bp == False,
                                    wrong_type_pval == False]
                if all(remove_row_tests):
                    if not args.print_only:
                        writer.writerows([row])
                else:
                    # print lines that are removed
                    logger.info('Removing record: {}'.format(row))

           
if __name__ == "__main__":
    main()
