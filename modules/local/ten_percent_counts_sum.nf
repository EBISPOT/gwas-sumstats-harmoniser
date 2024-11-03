process ten_percent_counts_sum {
    tag "$GCST"
    
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    val GCST
    
    output:
    tuple val(GCST), path("ten_percent_total_strand_count.tsv"), env(mode), env(s), emit: ten_sum

    shell:
    """
    sum_strand_counts_10percent_nf.py \
    -i ${launchDir}/$GCST/2_ten_sc \
    -o ten_percent_total_strand_count.tsv \
    -t ${params.threshold} 

    s=\$(cat ten_percent_total_strand_count.tsv | grep status | awk 'BEGIN {FS="\t"}; {print \$2}')
    mode=\$(grep palin_mode ten_percent_total_strand_count.tsv | cut -f2 )
    """
    // config.yaml  provide the threshold of the overall forward or reverse
}
