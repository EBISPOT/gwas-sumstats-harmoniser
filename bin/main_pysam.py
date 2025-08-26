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

from gwas_sumstats_tools.interfaces.data_table import SumStatsTable
from lib.SumStatRecord import SumStatRecord
from lib.VCFRecord import VCFRecord
from lib.Allele_utils import (
    is_palindromic, compatible_alleles_forward_strand, 
    compatible_alleles_reverse_strand, af_to_maf, afs_concordant, harmonise_palindromic,harmonise_forward_strand, harmonise_reverse_strand
)
from lib.VCF_utils import (
    extract_matching_record_from_vcf_records,
    get_vcf_records_0base, get_vcf_records
)
from lib.File_utils import (write_line_harmonise_output,
                            cpu_by_chromosome, get_code_table,
                            init_duckdb_table, chunked_iterable, open_gzip, parse_args
)

from lib.Duckdb_execute import safe_executemany, safe_copy

# ===== Globals for workers =====
args = None
tbx = None
header_order = None

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

    # === Harmonisation path ===
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
        na_token = str(getattr(args, "na_rep_out", "NA"))

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
                    safe_executemany(con, insert_sql, batch, na_token) 
                    batch.clear()

        if batch:
            safe_executemany(con, insert_sql, batch, na_token)

        delimiter = args.out_sep if args.out_sep is not None else '\t'
        compressed = str(args.hm_sumstats).endswith(".gz")
        safe_copy(con, "hm_stage", args.hm_sumstats, delimiter, compressed, "#NA")
        con.close()

        if args.hm_statfile:
            code_table = get_code_table()
            with open_gzip(args.hm_statfile, 'wb') as out_h:
                out_h.write('hm_code\tcount\tdescription\n'.encode('utf-8'))
                for key in sorted(code_counter.keys()):
                    desc = code_table.get(key, "Unknown code")
                    out_h.write(f"{key}\t{code_counter[key]}\t{desc}\n".encode('utf-8'))

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
        ss_rec = harmonise_palindromic(ss_rec, vcf_rec,
                                       args.palin_mode,
                                      args.af_vcf_field,
                                      args.infer_maf_threshold)

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
        out_line_dict = write_line_harmonise_output(ss_rec, vcf_rec, args._tag_neg_cached, args.na_rep_out)
        row = [out_line_dict.get(col) for col in header_order]
        local_rows.append(row)
    return local_rows, local_code_counter

def strand_count_worker(chunk):
    local_counter = Counter()
    for ss_rec in chunk:
        local_counter = strand_count_process(ss_rec, local_counter)
    return local_counter

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
