process harmonisation {
    publishDir "${launchDir}/$GCST/harmonization", mode: 'copy'
    queue 'short'
    memory { 5.GB * task.attempt }
    time { 3.hour * task.attempt }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' } // error caused by memory retry; others ignore
    maxRetries 3

    input:
    tuple val(GCST), val(palin_mode), val(status), val(chrom), path(merged), path(ref)

    output:
    tuple val(GCST), val(palin_mode), path("${chrom}.merged.hm"),path("${chrom}.merged.log.tsv.gz"), emit: hm_by_chrom

    when:
    status=="contiune"

    shell:
    """
    python ${params.script_path}/bin/sumstat_harmoniser/main_pysam.py \
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