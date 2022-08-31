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
    header_args=\$(python ${params.script_path}/bin/utils.py -f $merged -harm_args);
    python ${params.script_path}/bin/sumstat_harmoniser/main_pysam.py \
    --sumstats $merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    --hm_sumstats ${chrom}.merged.hm \
    --hm_statfile ${chrom}.merged.log.tsv.gz \
    \$header_args \
    --na_rep_in NA \
    --na_rep_out NA \
    --palin_mode $palin_mode
    """
}