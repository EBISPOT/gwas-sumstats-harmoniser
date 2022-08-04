process generate_strand_counts {
    publishDir "${launchDir}/$GCST/all_sc", mode: 'copy'
    queue 'short'
    memory { 5.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' } // error caused by memory retry; others ignore
    maxRetries 3

    input:
    tuple val(GCST), val(chrom), path(merged), path(vcf), val(status)

    output:
    tuple val(GCST), val(status), path("full_${chrom}.sc"), emit: all_sc

    when:
    status=="rerun"

    shell:
    """
    python ${params.script_path}/bin/sumstat_harmoniser/main_pysam.py \
    --sumstats $merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    --chrom_col chromosome \
    --pos_col base_pair_location \
    --effAl_col effect_allele \
    --otherAl_col other_allele \
    --rsid_col variant_id \
    --strand_counts full_${chrom}.sc
    """
}