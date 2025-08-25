#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
# YUE - Using pysam to replace tabix in subprocess
#
# Harmonise GWAS summary statistics against a reference VCF

import sys
import gzip
import argparse
from collections import OrderedDict, Counter
from multiprocessing import Pool, Manager, cpu_count
from cyvcf2 import VCF
from itertools import islice
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import duckdb

from gwas_sumstats_tools.interfaces.data_table import SumStatsTable
from lib.SumStatRecord import SumStatRecord
from lib.VCFRecord import VCFRecord
from lib.Allele_utils import (
    is_palindromic, compatible_alleles_forward_strand, 
    compatible_alleles_reverse_strand, af_to_maf, afs_concordant
)
from lib.VCF_utils import (
    extract_matching_record_from_vcf_records,
    get_vcf_records_0base, get_vcf_records
)
from lib.File_utils import (write_line_harmonise_output,
    convert_arg_separator, cpu_by_chromosome, get_code_table,
    init_duckdb_table, chunked_iterable
)

# ===== Globals for workers =====
args = None
tbx = None
header_order = None

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1),
       retry=retry_if_exception_type(duckdb.IOException), reraise=True)
def safe_executemany(con, sql, batch):
    con.executemany(sql, batch)

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1),
       retry=retry_if_exception_type(duckdb.IOException), reraise=True)
def safe_commit(con):
    con.execute("COMMIT")

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1),
       retry=retry_if_exception_type(duckdb.IOException), reraise=True)
def safe_copy(con, table_name, out_path, delimiter, compressed):
    copy_opts = "FORMAT CSV, HEADER, DELIMITER ?"
    if compressed:
        copy_opts += ", COMPRESSION 'gzip'"
    con.execute(f'COPY {table_name} TO ? ({copy_opts})', [out_path, delimiter])

def main():
    # Prepare data 
    # Get args
    global args, header_order
    args = parse_args()
    strand_counter = Counter()
    code_counter = Counter()

    procs = cpu_by_chromosome(args.chrom)

    # === Strand counts path ===
    if args.strand_counts:
        with Pool(processes=procs,
                  initializer=_init_worker,
                  initargs=(args.vcf, args, None),
                  maxtasksperchild=200) as pool:
            for result in pool.imap_unordered(strand_count_worker,
                                              chunked_iterable(yield_sum_stat_records(args.sumstats, args.in_sep), 5000),
                                              chunksize=4):
                strand_counter.update(result)

        # write to file
        with open_gzip(args.strand_counts, rw='wb') as out_h:
            for key in sorted(strand_counter.keys()):
                out_h.write(f"{key}\t{strand_counter[key]}\n".encode("utf-8"))
        return
    
    if args.hm_sumstats:
        # Build header order ONCE
        out_header = SumStatsTable(sumstats_file=args.sumstats)._set_header_order()
        
        # remove neg_log_10_p_value if you want p_value output
        tag_neg = False
        if "neg_log_10_p_value" in out_header:
            out_header.remove("neg_log_10_p_value")
            tag_neg = True

        # add generated columns if missing
        for col in ["hm_code", "variant_id", "rsid"]:
            if col not in out_header:
                out_header.append(col)
        
        # write header
        # ---- Step 2: "write the header" by creating the table ----
        # Use :memory: to keep it in RAM. Change to "hm_stage.duckdb" to persist as a DB file.
        # Need to make sure the memory is 5x the size of the input file, otherwise, it needs tmp file
        args._tag_neg_cached = tag_neg
        header_order = out_header

        con, insert_sql = init_duckdb_table(header_order, table_name="hm_stage", database=":memory:")
        con.execute("PRAGMA disable_progress_bar")
        con.execute("BEGIN")

        BATCH_SIZE = 20000
        batch = []

        with Pool(processes=procs,
                  initializer=_init_worker,
                  initargs=(args.vcf, args, header_order),
                  maxtasksperchild=200) as pool:
            for rows, local_code_counter in pool.imap_unordered(
                harmonise_worker,
                chunked_iterable(yield_sum_stat_records(args.sumstats, args.in_sep), 5000),
                chunksize=4
            ):
                code_counter.update(local_code_counter)
                batch.extend(rows)
                if len(batch) >= BATCH_SIZE:
                    safe_executemany(con, insert_sql, batch) 
                    batch.clear()

        if batch:
            safe_executemany(con, insert_sql, batch)
        safe_commit(con)

        delimiter = args.out_sep if args.out_sep is not None else '\t'
        compressed = str(args.hm_sumstats).endswith(".gz")
        safe_commit(con)
        safe_copy(con, "hm_stage", args.hm_sumstats, delimiter, compressed)
        con.close()

        if args.hm_statfile:
            code_table = get_code_table()
            with open_gzip(args.hm_statfile, 'wb') as out_h:
                out_h.write('hm_code\tcount\tdescription\n'.encode('utf-8'))
                for key in sorted(code_counter.keys()):
                    desc = code_table.get(key, "Unknown code")
                    out_h.write(f"{key}\t{code_counter[key]}\t{desc}\n".encode('utf-8'))
def parse_args():
    """ Parse command line args using argparse.
    """
    parser = argparse.ArgumentParser(description="Summary statistc harmoniser")

    # Input file args
    infile_group = parser.add_argument_group(title='Input files')
    infile_group.add_argument('--sumstats', metavar="<file>",
                        help=('GWAS summary statistics file'), type=str,
                        required=True)
    infile_group.add_argument('--vcf', metavar="<file>",
                        help=('Reference VCF file. Use # as chromosome wildcard.'), type=str, required=True)

    # Output file args
    outfile_group = parser.add_argument_group(title='Output files')
    outfile_group.add_argument('--hm_sumstats', metavar="<file>",
        help=("Harmonised sumstat output file (use 'gz' extension to gzip)"), type=str)
    outfile_group.add_argument('--hm_statfile', metavar="<file>",
        help=("Statistics from harmonisation process output file. Should only be used in conjunction with --hm_sumstats."), type=str)
    outfile_group.add_argument('--strand_counts', metavar="<file>",
        help=("Output file showing number of variants that are forward/reverse/palindromic"), type=str)

    # Harmonisation mode
    mode_group = parser.add_argument_group(title='Harmonisation mode')
    mode_group.add_argument('--coordinate',
                            help=('coordinate system of the input file:\n'
                                  '(a) 1_base '
                                  '(b) 0_base '),
                            choices=['1-based', '0-based'],
                            nargs='?',
                            type=str,
                            default="1-based")
    mode_group.add_argument('--palin_mode', metavar="[infer|forward|reverse|drop]",
                        help=('Mode to use for palindromic variants:\n'
                              '(a) infer strand from effect-allele freq, '
                              '(b) assume forward strand, '
                              '(c) assume reverse strand, '
                              '(d) drop palindromic variants'),
                        choices=['infer', 'forward', 'reverse', 'drop'],
                        type=str)

    # Infer strand specific options
    infer_group = parser.add_argument_group(title='Strand inference options',
        description='Options that are specific to strand inference (--palin_mode infer)')
    infer_group.add_argument('--af_vcf_field', metavar="<str>",
                        help=('VCF info field containing alt allele freq (default: AF_NFE)'),
                        type=str, default="AF_NFE")
    infer_group.add_argument('--infer_maf_threshold', metavar="<float>",
                        help=('Max MAF that will be used to infer palindrome strand (default: 0.42)'),
                        type=float, default=0.42)

    # Global column args
    incols_group = parser.add_argument_group(title='Input column names')
    incols_group.add_argument('--chrom_col', metavar="<str>",
                        help=('Chromosome column'), type=str, required=True)
    incols_group.add_argument('--pos_col', metavar="<str>",
                        help=('Position column'), type=str, required=True)
    incols_group.add_argument('--effAl_col', metavar="<str>",
                        help=('Effect allele column'), type=str, required=True)
    incols_group.add_argument('--otherAl_col', metavar="<str>",
                        help=('Other allele column'), type=str, required=True)
    incols_group.add_argument('--beta_col', metavar="<str>",
                        help=('beta column'), type=str)
    incols_group.add_argument('--zscore_col', metavar="<str>",
                        help=('Z-score column'), type=str)
    incols_group.add_argument('--or_col', metavar="<str>",
                        help=('Odds ratio column'), type=str)
    incols_group.add_argument('--or_col_lower', metavar="<str>",
                        help=('Odds ratio lower CI column'), type=str)
    incols_group.add_argument('--or_col_upper', metavar="<str>",
                        help=('Odds ratio upper CI column'), type=str)
    incols_group.add_argument('--eaf_col', metavar="<str>",
                        help=('Effect allele frequency column'), type=str)
    incols_group.add_argument('--rsid_col', metavar="<str>",
                        help=('rsID column in the summary stat file'), type=str)
    incols_group.add_argument('--hm_coordinate_conversion', metavar="<str>",
                        help=('liftover method column'), type=str, required=True)

    # Global other args
    other_group = parser.add_argument_group(title='Other args')
    other_group.add_argument('--only_chrom', metavar="<str>",
                        help=('Only process this chromosome'), type=str)
    other_group.add_argument('--chrom', metavar="<str>",
                        help=('The chromosome processed in the step'), type=str)
    other_group.add_argument('--in_sep', metavar="<str>",
                        help=('Input file column separator [tab|space|comma|other] (default: tab)'),
                        type=str, default='tab')
    other_group.add_argument('--out_sep', metavar="<str>",
                        help=('Output file column separator [tab|space|comma|other] (default: tab)'),
                        type=str, default='tab')
    other_group.add_argument('--na_rep_in', metavar="<str>",
                        help=('How NA  are represented in the input file (default: "")'),
                        type=str, default="")
    other_group.add_argument('--na_rep_out', metavar="<str>",
                        help=('How to represent NA values in output (default: "")'),
                        type=str, default="")
    other_group.add_argument('--chrom_map', metavar="<str>",
                        help=('Map summary stat chromosome names, e.g. `--chrom_map 23=X 24=Y`'),
                        type=str, nargs='+')

    # Parse arguments
    args = parser.parse_args()

    # Convert input/output separators
    args.in_sep = convert_arg_separator(args.in_sep)
    args.out_sep = convert_arg_separator(args.out_sep)

    # Assert that at least one of --hm_sumstats, --strand_counts is selected
    assert any([args.hm_sumstats, args.strand_counts]), \
        "Error: at least 1 of --hm_sumstats, --strand_counts must be selected"

    # Assert that --hm_statfile is only ever used in conjunction with --hm_sumstats
    if args.hm_statfile:
        assert args.hm_sumstats, \
        "Error: --hm_statfile must only be used in conjunction with --hm_sumstats"

    # Assert that mode is selected if doing harmonisation
    if args.hm_sumstats:
        assert args.palin_mode, \
        "Error: '--palin_mode' must be used with '--hm_sumstats'"

    # Assert that inference specific options are supplied
    if args.palin_mode == 'infer':
        assert all([args.af_vcf_field, args.infer_maf_threshold, args.eaf_col]), \
            "Error: '--af_vcf_field', '--infer_maf_threshold' and '--eaf_col' must be used with '--palin_mode infer'"

    # Assert that OR_lower and OR_upper are used both present if any
    if any([args.or_col_lower, args.or_col_upper]):
        assert all([args.or_col_lower, args.or_col_upper]), \
        "Error: '--or_col_lower' and '--or_col_upper' must be used together"

    # Parse chrom_map
    if args.chrom_map:
        try:
            chrom_map_d = dict([pair.split('=') for pair in args.chrom_map])
            args.chrom_map = chrom_map_d
        except ValueError:
            assert False, \
            'Error: --chrom_map must be in the format `--chrom_map 23=X 24=Y`'

    return args

def _init_worker(vcf_path, _args, _header_order):
    global tbx, args, header_order
    tbx = VCF(vcf_path)          # open per process (fast, thread-safe in process)
    args = _args                 # pass parsed args (must be picklable)
    header_order = _header_order  # pass header order (must be picklable)

def extract_vcf_record(ss_rec, coordinate):
    if int(coordinate[0])==0 and ss_rec.lifmethod=="lo":
        if len(str(ss_rec.effect_al))+len(str(ss_rec.other_al))>2: 
            vcf_recs =get_vcf_records_0base(
                tbx,
                ss_rec.chrom,
                ss_rec.pos)
        else:
            vcf_recs = get_vcf_records(
                tbx,
                ss_rec.chrom,
                ss_rec.pos)
    else:
        vcf_recs = get_vcf_records(
            tbx,
            ss_rec.chrom,
            ss_rec.pos)
    
    # Extract the VCF record that matches the summary stat record
    vcf_rec, ret_code = extract_matching_record_from_vcf_records(
                ss_rec, vcf_recs)
    
    return vcf_rec, ret_code

def strand_count_process(ss_rec, strand_counter):
    # If set to only process 1 chrom, skip none matching chroms
    if args.only_chrom and not args.only_chrom == ss_rec.chrom:
        return strand_counter

    # Validate summary stat record
    ret_code = ss_rec.validate_ssrec()
    if ret_code:
        ss_rec.hm_code = ret_code
        strand_counter['Invalid variant for harmonisation'] += 1

    # Skip rows that have code 14 (fail validation)
    if not ss_rec.hm_code:
        # Get VCF reference variants for this recordls
        vcf_rec, ret_code = extract_vcf_record(ss_rec, args.coordinate)

        # Set return code when vcf_rec was not found
        if ret_code:
            ss_rec.hm_code = ret_code

        # If vcf record was found, extract some required values
        if vcf_rec:
            # Get alt allele
            vcf_alt = vcf_rec.alt_als[0]

    else:
        vcf_rec = None

   
    # Skip if harmonisation code exists (no VCF record exists or code 14)
    if ss_rec.hm_code:
        strand_counter['No VCF record found'] += 1

    # palindromic alleles
    elif is_palindromic(ss_rec.other_al, ss_rec.effect_al):
        strand_counter['Palindromic variant'] += 1

        # Harmonise opposite strand alleles
    elif compatible_alleles_reverse_strand(ss_rec.other_al,
                                               ss_rec.effect_al,
                                               vcf_rec.ref_al,
                                               vcf_alt):

        strand_counter['Reverse strand variant'] += 1

        # Harmonise same forward alleles
    elif compatible_alleles_forward_strand(ss_rec.other_al,
                                               ss_rec.effect_al,
                                               vcf_rec.ref_al,
                                               vcf_alt):

        strand_counter['Forward strand variant'] += 1

        # Should never reach this 'else' statement
    else:
            sys.exit("Error: Alleles were not palindromic, opposite strand, or "
                     "same strand!")
    
    return strand_counter

def harmonise_process(ss_rec):
    # If set to only process 1 chrom, skip none matching chroms
    if args.only_chrom and not args.only_chrom == ss_rec.chrom:
        vcf_rec = None
        return ss_rec, vcf_rec

    # Validate summary stat record
    ret_code = ss_rec.validate_ssrec()
    if ret_code:
        ss_rec.hm_code = ret_code

    vcf_rec = None
    # Skip rows that have code 14 (fail validation)
    if not ss_rec.hm_code:
        # Extract the VCF record that matches the summary stat record
        vcf_rec, ret_code = extract_vcf_record(ss_rec, args.coordinate)

        # Set return code when vcf_rec was not found
        if ret_code:
            ss_rec.hm_code = ret_code

        # If vcf record was found, extract some required values
        if vcf_rec:
            # Get alt allele
            vcf_alt = vcf_rec.alt_als[0]
            # Set variant information from VCF file
            ss_rec.hm_rsid = vcf_rec.id
            ss_rec.hm_chrom = vcf_rec.chrom
            ss_rec.hm_pos = vcf_rec.pos
            ss_rec.hm_other_al = vcf_rec.ref_al
            ss_rec.hm_effect_al = vcf_alt

    # Skip if harmonisation code exists (no VCF record exists or code 14)
    if ss_rec.hm_code:
        ss_rec.is_harmonised = False
        return ss_rec, vcf_rec

    # Harmonise palindromic alleles
    elif is_palindromic(ss_rec.other_al, ss_rec.effect_al):
        ss_rec = harmonise_palindromic(ss_rec, vcf_rec)

    # Harmonise opposite strand alleles
    elif compatible_alleles_reverse_strand(ss_rec.other_al,
                                               ss_rec.effect_al,
                                               vcf_rec.ref_al,
                                               vcf_alt):
        ss_rec = harmonise_reverse_strand(ss_rec, vcf_rec)

    # Harmonise same forward alleles
    elif compatible_alleles_forward_strand(ss_rec.other_al,
                                               ss_rec.effect_al,
                                               vcf_rec.ref_al,
                                               vcf_alt):
        
        ss_rec = harmonise_forward_strand(ss_rec, vcf_rec)

    # Should never reach this 'else' statement
    else:
        sys.exit("Error: Alleles were not palindromic, opposite strand, or "
                     "same strand!")
    return ss_rec, vcf_rec

def harmonise_worker(chunk):
    local_rows = []
    local_code_counter = Counter()
    for ss_rec in chunk:
        ss_rec, vcf_rec = harmonise_process(ss_rec)
        if ss_rec and ss_rec.hm_code is not None:
            local_code_counter[ss_rec.hm_code] += 1
        out_line_dict = write_line_harmonise_output(ss_rec, vcf_rec, args._tag_neg_cached, header_order)
        row = [out_line_dict.get(col) for col in header_order]
        local_rows.append(row)
    return local_rows, local_code_counter

def strand_count_worker(chunk):
    local_counter = Counter()
    for ss_rec in chunk:
        local_counter = strand_count_process(ss_rec, local_counter)
    return local_counter

def harmonise_palindromic(ss_rec, vcf_rec):
    ''' Harmonises palindromic variant
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_rec (VCFRecord): matching vcf record
    Returns:
        harmonised ss_rec
    '''

    # Mode: Infer strand mode
    if args.palin_mode == 'infer':

        # Extract allele frequency if argument is provided
        if args.af_vcf_field and args.af_vcf_field in vcf_rec.info:
            vcf_alt_af = float(vcf_rec.info[args.af_vcf_field][0])
        else:
            ss_rec.hm_code = 17
            return ss_rec

        # Discard if either MAF is greater than threshold
        if ss_rec.eaf:
            if ( af_to_maf(ss_rec.eaf) > args.infer_maf_threshold or
                af_to_maf(vcf_alt_af) > args.infer_maf_threshold ):
                ss_rec.hm_code = 18
                return ss_rec
        else:
            ss_rec.hm_code = 17
            return ss_rec

        # If EAF and alt AF are concordant, then alleles are on forward strand
        if afs_concordant(ss_rec.eaf, vcf_alt_af):

            # If alleles flipped orientation
            if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
                ss_rec.flip_beta()
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 2
                return ss_rec
            # If alleles in correct orientation
            else:
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 1
                return ss_rec

        # Else alleles are on the reverse strand
        else:

            # Take reverse complement of ssrec alleles
            ss_rec.revcomp_alleles()
            # If alleles flipped orientation
            if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
                ss_rec.flip_beta()
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 4
                return ss_rec
            # If alleles in correct orientation
            else:
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 3
                return ss_rec

    # Mode: Assume palindromic variants are on the forward strand
    elif args.palin_mode == 'forward':

        # If alleles flipped orientation
        if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
            ss_rec.flip_beta()
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 6
            return ss_rec
        # If alleles in correct orientation
        else:
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 5
            return ss_rec

    # Mode: Assume palindromic variants are on the reverse strand
    elif args.palin_mode == 'reverse':

        # Take reverse complement of ssrec alleles
        ss_rec.revcomp_alleles()
        # If alleles flipped orientation
        if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
            ss_rec.flip_beta()
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 8
            return ss_rec
        # If alleles in correct orientation
        else:
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 7
            return ss_rec

    # Mode: Drop palindromic variants
    elif args.palin_mode == 'drop':
        ss_rec.hm_code = 9
        return ss_rec

def harmonise_reverse_strand(ss_rec, vcf_rec):
    ''' Harmonises reverse strand variant
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_rec (VCFRecord): matching vcf record
    Returns:
        harmonised ss_rec
    '''
    # Take reverse complement of ssrec alleles
    ss_rec.revcomp_alleles()
    # If alleles flipped orientation
    if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
        ss_rec.flip_beta()
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 13
        return ss_rec
    # If alleles in correct orientation
    else:
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 12
        return ss_rec

def harmonise_forward_strand(ss_rec, vcf_rec):
    ''' Harmonises forward strand variant
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_rec (VCFRecord): matching vcf record
    Returns:
        harmonised ss_rec
    '''
    # If alleles flipped orientation
    if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
        ss_rec.flip_beta()
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 11
        return ss_rec
    # If alleles in correct orientation
    else:
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 10
        return ss_rec

def yield_sum_stat_records(inf, sep):
    """ Load lines from summary stat file and convert to SumStatRecord class.
    Args:
        inf (str): input file
        sep (str): column separator

    Returns:
        SumStatRecord
    """
    for row in parse_sum_stats(inf, sep):

        # Replace chrom with --chrom_map value
        chrom = row[args.chrom_col]
        if args.chrom_map:
            chrom = args.chrom_map.get(chrom, chrom)
        # Make sumstat class instance
        ss_record = SumStatRecord(chrom,
                                  row[args.pos_col],
                                  row[args.otherAl_col],
                                  row[args.effAl_col],
                                  row.get(args.beta_col, None),
                                  row.get(args.zscore_col, None),
                                  row.get(args.or_col, None),
                                  row.get(args.or_col_lower, None),
                                  row.get(args.or_col_upper, None),
                                  row.get(args.eaf_col, None),
                                  row.get(args.rsid_col, None),
                                  row,
                                  row[args.hm_coordinate_conversion])
        yield ss_record

def str2bool(v):
    """ Parses argpare boolean input
    """
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_sum_stats(inf, sep):
    """ Yields a line at a time from the summary statistics file.
    Args:
        inf (str): input file
        sep (str): column separator

    Returns:
        OrderedDict: {column: value}
    """
    with open_gzip(inf, "rb") as in_handle:
        # Get header
        header = in_handle.readline().decode("utf-8").rstrip().split(sep)
        # Assert that all column arguments are contained in header
        for arg, value in args.__dict__.items():
            if '_col' in arg and value:
                assert value in header, \
                'Error: --{0} {1} not found in input header'.format(arg, value)
        # Iterate over lines
        for line in in_handle:
            values = line.decode("utf-8").rstrip().split(sep)
            # Replace any na_rep_in values with None
            values = [value if value != args.na_rep_in else None
                      for value in values]
            # Check we have the correct number of elements
            assert len(values) == len(header), 'Error: column length ({0}) does not match header length ({1})'.format(len(values), len(header))
            yield OrderedDict(zip(header, values))

if __name__ == '__main__':

    main()
