import duckdb
import gzip
from itertools import islice
from multiprocessing import cpu_count
import argparse

# === File and data utilities ===
def open_gzip(inf, rw="rb"):
    """ Returns handle using gzip if gz file extension.
    """
    if inf.split(".")[-1] == "gz":
        return gzip.open(inf, rw)
    else:
        return open(inf, rw)

def convert_arg_separator(s):
    ''' Converts [tab|space|comma|other] to a variable
    '''
    if s == 'tab':
        return '\t'
    elif s == 'space':
        return ' '
    elif s == 'comma':
        return ','
    else:
        return s

def cpu_by_chromosome(chr):
    total = max(cpu_count() - 1, 1)

    # 2) auto-detect by size
    chrom = str (chr).upper() if chr else "other" 
    
    # Relative weight for each chromosome size class
    size_weight = {
        # big autosomes
        "1": 1.0, "2": 1.0, "3": 1.0, "4": 1.0, "5": 1.0, "6": 1.0, "7": 1.0,
        # medium
        "8": 0.8, "9": 0.8, "10": 0.8, "11": 0.8, "12": 0.8,
        # smaller
        "13": 0.5, "14": 0.5, "15": 0.5, "16": 0.5, "17": 0.5, "18": 0.5,
        # small/tiny
        "19": 0.3, "20": 0.3, "21": 0.3, "22": 0.3,
        # sex/mt
        "X": 0.2, "Y": 0.2, "MT": 0.2,
        # fallback
        "other": 0.2
    }

    weight = size_weight.get(chrom , size_weight["other"])
    procs = max(1, int(total * weight))

    # Always leave 1 CPU free for the OS
    return max(1, min(procs, total - 1))

# === Constants ===
def get_code_table():
    """Returns harmonization code descriptions"""
    return {
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
        18: 'Palindromic; Infer strand; EAF < --maf_palin_threshold; Will not harmonise' 
    }

def init_duckdb_table(out_header, table_name="hm_stage", database=":memory:"):
    """
    out_header: list[str] of column names (your computed header order)
    type_hints: optional dict[str, str] mapping column -> DuckDB SQL type
                e.g. {"hm_code":"INTEGER","p_value":"DOUBLE","beta":"DOUBLE","se":"DOUBLE"}
    database: ":memory:" for in-memory DB. Use a filepath to persist as a DB file.
    """
    type_hints = {
            "chromosome": "VARCHAR",
            "base_pair_location": "BIGINT",
            "effect_allele": "VARCHAR",
            "other_allele": "VARCHAR",
            "standard_error": "DOUBLE",
            "effect_allele_frequency": "DOUBLE",
            "p_value": "DOUBLE",
            "hm_coordinate_conversion": "VARCHAR",
            "hm_code": "INTEGER",
            "beta": "DOUBLE",
            "odds_ratio": "DOUBLE",
            "z_score": "DOUBLE",
            "ci_lower": "DOUBLE",
            "ci_upper": "DOUBLE",
            "ref_allele": "VARCHAR",
            "n": "BIGINT",
            }
    
    # ---- Step 1: init DuckDB (in-memory) + create table from header ----
    con = duckdb.connect(database=database)
    type_hints = type_hints or {}

    # Only columns that appear in out_header; default to VARCHAR
    col_defs = []
    for col in out_header:
        col_type = type_hints.get(col, "VARCHAR")
        col_defs.append(f'"{col}" {col_type}')
    
    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(col_defs)});'
    con.execute(create_sql)

    # Prepared INSERT statement
    placeholders = ", ".join(["?"] * len(out_header))
    insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
    return con, insert_sql

def write_line_harmonise_output(ss_rec, vcf_rec, tag_neg, na_rep):
    # write from vcf records
    harmonised_ok = bool(vcf_rec and getattr(ss_rec, "is_harmonised", False))
    
    # Precompute common fields based on vcf_rec
    na = na_rep
    chrom = ss_rec.hm_chrom if harmonised_ok else na
    pos   = ss_rec.hm_pos if harmonised_ok else na
    ea    = ss_rec.hm_effect_al.str() if harmonised_ok else na
    oa    = ss_rec.hm_other_al.str()  if harmonised_ok else na
    rsid  = ss_rec.hm_rsid if harmonised_ok else na
    varid = (vcf_rec.hgvs()[0] if (harmonised_ok and vcf_rec.hgvs()[0] is not None) else na)

    # Precompute common fields based on if the data is harmonised from ss_rec
    harmo_tag = getattr(ss_rec, "is_harmonised", False)
    beta = ss_rec.beta if harmo_tag and ss_rec.beta is not None else na
    oddsr = ss_rec.oddsr if harmo_tag and ss_rec.oddsr is not None else na
    zscore = ss_rec.zscore if harmo_tag and ss_rec.zscore is not None else na
    ci_low = ss_rec.oddsr_lower if harmo_tag and ss_rec.oddsr_lower is not None else na
    ci_up = ss_rec.oddsr_upper if harmo_tag and ss_rec.oddsr_upper is not None else na
    eaf = ss_rec.eaf if harmo_tag and ss_rec.eaf is not None else na

    # Process the neg_log_10_p_value
    if tag_neg == True:
        try:
            nl10p = ss_rec.data.get("neg_log_10_p_value")
            pval = 10 ** (float(nl10p) * -1) if nl10p is not None else na
        except Exception:
            pval = na
    else:
        pval = ss_rec.data.get("p_value", na)

    # Build a dict only for keys we might need; everything else NA-filled later
    base = {
        "chromosome": chrom,
        "base_pair_location": pos,
        "effect_allele": ea,
        "other_allele": oa,
        "rsid": rsid,
        "variant_id": varid,
        "hm_code": ss_rec.hm_code,
        "beta": beta,
        "odds_ratio": oddsr,
        "z_score": zscore,
        "ci_lower": ci_low,
        "ci_upper": ci_up,
        "hm_coordinate_conversion": ss_rec.data.get("hm_coordinate_conversion", na),
        "standard_error": ss_rec.data.get("standard_error") or na,
        "effect_allele_frequency": eaf,
        "p_value": pval,
    }

    # Add other data from summary stat file
    for key, value in ss_rec.data.items():
        if key not in base:
            base[key] = value if value is not None else na

    return base

def chunked_iterable(iterable, size):
    """Yield lists of size `size` from an iterable."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

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