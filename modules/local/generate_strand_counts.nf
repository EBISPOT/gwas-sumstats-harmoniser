process generate_strand_counts {
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"
        
    input:
    tuple val(GCST), val(chrom), path(merged), path(vcf), val(status)

    output:
    tuple val(GCST), val(status), path("full_${chrom}.sc"), emit: all_sc

    when:
    status=="rerun"

    shell:
    """
    header_args=\$(utils.py -f $merged -strand_count_args);
    main_pysam.py \
    --sumstats $merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    \$header_args \
    --strand_counts full_${chrom}.sc
    """
}
