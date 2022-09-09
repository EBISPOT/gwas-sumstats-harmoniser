process harmonization {

    input:
    tuple val(GCST), val(palin_mode), val(status), val(chrom), path(merged), path(ref)

    output:
    tuple val(GCST), val(palin_mode), path("${chrom}.merged.hm"),path("${chrom}.merged.log.tsv.gz"), emit: hm_by_chrom

    when:
    status=="contiune"

    shell:
    """
    main_pysam.py \
    --sumstats $merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    --hm_sumstats ${chrom}.merged.hm \
    --hm_statfile ${chrom}.merged.log.tsv.gz \
    --chrom_col chromosome \
    --pos_col base_pair_location \
    --effAl_col effect_allele \
    --otherAl_col other_allele \
    --rsid_col variant_id \
    --beta_col beta \
    --or_col odds_ratio \
    --or_col_lower ci_lower \
    --or_col_upper ci_upper \
    --eaf_col effect_allele_frequency \
    --na_rep_in NA \
    --na_rep_out NA \
    --palin_mode $palin_mode
    """
}