process summarise_strand_counts {
    tag "$GCST"
    
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    tuple val(GCST), val(status)
    
    output:
    tuple val(GCST), path("total_strand_count.tsv"), env(palin_mode), val("contiune"), emit: all_sum

    when:
    status=="rerun"

    shell:
    """ 
    sum_strand_counts_nf.py \
    -i ${launchDir}/$GCST/3_all_sc \
    -o total_strand_count.tsv \
    -t ${params.threshold}

    palin_mode=\$(grep palin_mode total_strand_count.tsv| cut -f2 )
    """
}