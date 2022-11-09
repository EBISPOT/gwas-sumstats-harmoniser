SNP_DSET = 'variant_id'
MANTISSA_DSET = 'mantissa'
EXP_DSET = 'exponent'
PVAL_DSET = 'p_value'
STUDY_DSET = 'study_accession'
CHR_DSET = 'chromosome'
BP_DSET = 'base_pair_location'
OR_DSET = 'odds_ratio'
RANGE_U_DSET = 'ci_upper'
RANGE_L_DSET = 'ci_lower'
BETA_DSET = 'beta'
RSID = 'rsid'
SE_DSET = 'standard_error'
EFFECT_DSET = 'effect_allele'
OTHER_DSET = 'other_allele'
FREQ_DSET = 'effect_allele_frequency'
HM_CC_DSET = 'hm_coordinate_conversion'
HM_OR_DSET = 'hm_odds_ratio'
HM_RANGE_U_DSET = 'hm_ci_upper'
HM_RANGE_L_DSET = 'hm_ci_lower'
HM_BETA_DSET = 'hm_beta'
HM_EFFECT_DSET = 'hm_effect_allele'
HM_OTHER_DSET = 'hm_other_allele'
HM_FREQ_DSET = 'hm_effect_allele_frequency'
HM_VAR_ID = 'hm_variant_id'
HM_CODE = 'hm_code'


DSET_TYPES = {SNP_DSET: str, PVAL_DSET: float, MANTISSA_DSET: float, EXP_DSET: int, STUDY_DSET: str,
              CHR_DSET: int, BP_DSET: int, OR_DSET: float, RANGE_U_DSET: float, RANGE_L_DSET: float, BETA_DSET: float, SE_DSET: float,
              EFFECT_DSET: str, OTHER_DSET: str, FREQ_DSET: float, HM_EFFECT_DSET: str,
              HM_OTHER_DSET: str, HM_BETA_DSET: float, HM_OR_DSET: float, HM_FREQ_DSET: float, HM_CODE: int,
              HM_VAR_ID: str, HM_RANGE_L_DSET: float, HM_RANGE_U_DSET: float, HM_CC_DSET: str}

REFERENCE_DSET = MANTISSA_DSET
HARMONISATION_PREFIX = 'hm_'
GWAS_CATALOG_STUDY_PREFIX = 'GCST'

TO_DISPLAY_DEFAULT = {SNP_DSET, PVAL_DSET, STUDY_DSET, CHR_DSET, BP_DSET, HM_OR_DSET, HM_RANGE_L_DSET, HM_RANGE_U_DSET,
                      HM_BETA_DSET, HM_EFFECT_DSET, HM_OTHER_DSET, HM_FREQ_DSET, HM_CODE}

TO_DISPLAY_RAW = {SNP_DSET, PVAL_DSET, STUDY_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_L_DSET, RANGE_U_DSET, BETA_DSET,
                  SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET}


TO_LOAD_DSET_HEADERS_DEFAULT = {SNP_DSET, PVAL_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_L_DSET, RANGE_U_DSET, BETA_DSET,
                        SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET, HM_OR_DSET, HM_RANGE_L_DSET, HM_RANGE_U_DSET, HM_BETA_DSET,
                                HM_EFFECT_DSET, HM_OTHER_DSET, HM_FREQ_DSET, HM_CODE}
TO_STORE_DSETS_DEFAULT = {SNP_DSET, MANTISSA_DSET, EXP_DSET, STUDY_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_L_DSET, RANGE_U_DSET,
                  BETA_DSET, SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET, HM_OR_DSET, HM_RANGE_L_DSET, HM_RANGE_U_DSET, HM_BETA_DSET,
                                HM_EFFECT_DSET, HM_OTHER_DSET, HM_FREQ_DSET, HM_VAR_ID, HM_CODE}
TO_QUERY_DSETS_DEFAULT = {SNP_DSET, MANTISSA_DSET, EXP_DSET, STUDY_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_L_DSET, RANGE_U_DSET, BETA_DSET,
                  SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET, HM_OR_DSET, HM_RANGE_L_DSET, HM_RANGE_U_DSET, HM_BETA_DSET,
                                HM_EFFECT_DSET, HM_OTHER_DSET, HM_FREQ_DSET, HM_VAR_ID, HM_CODE}
TO_INDEX = {SNP_DSET, PVAL_DSET, CHR_DSET, BP_DSET}
REQUIRED = {CHR_DSET, PVAL_DSET, SNP_DSET}#, EFFECT_DSET, OTHER_DSET}

HARMONISER_ARG_MAP = {
    CHR_DSET: "--chrom_col",
    BP_DSET: "--pos_col",
    EFFECT_DSET: "--effAl_col",
    OTHER_DSET: "--otherAl_col",
    SNP_DSET: "--rsid_col",
    BETA_DSET: "--beta_col",
    OR_DSET: "--or_col",
    RANGE_L_DSET: "--or_col_lower",
    RANGE_U_DSET: "--or_col_upper",
    FREQ_DSET: "--eaf_col"
}

DEFAULT_CHROMS = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT']