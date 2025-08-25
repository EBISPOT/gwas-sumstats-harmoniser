import duckdb
from multiprocessing import cpu_count

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

def write_line_harmonise_output(ss_rec, vcf_rec, tag_neg, out_header):
    # write from vcf records
    harmonised_ok = bool(vcf_rec and getattr(ss_rec, "is_harmonised", False))
    # Precompute common fields
    na = args.na_rep_out
    chrom = ss_rec.hm_chrom if harmonised_ok else na
    pos   = ss_rec.hm_pos if harmonised_ok else na
    ea    = ss_rec.hm_effect_al.str() if harmonised_ok else na
    oa    = ss_rec.hm_other_al.str()  if harmonised_ok else na
    rsid  = ss_rec.hm_rsid if harmonised_ok else na
    varid = (vcf_rec.hgvs()[0] if (harmonised_ok and vcf_rec.hgvs()[0] is not None) else na)

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
        "beta": ss_rec.data.get("beta", na),
        "odds_ratio": ss_rec.data.get("oddsr", na),
        "z_score": ss_rec.data.get("zscore", na),
        "ci_lower": ss_rec.data.get("oddsr_lower", na),
        "ci_upper": ss_rec.data.get("oddsr_upper", na),
        "hm_coordinate_conversion": ss_rec.data.get("hm_coordinate_conversion", na),
        "standard_error": ss_rec.data.get("standard_error") or na,
        "effect_allele_frequency": (ss_rec.eaf if (ss_rec.eaf is not None and getattr(ss_rec, "is_harmonised", False)) else na),
        "p_value": pval,
    }

    # Add other data from summary stat file
    for key, value in ss_rec.data.items():
        if key not in base:
            base[key] = value if value is not None else na

    return [base.get(col, na) for col in out_header]

def chunked_iterable(iterable, size):
    """Yield lists of size `size` from an iterable."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk