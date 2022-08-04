process summarise_strand_counts {
    publishDir "${launchDir}/$GCST", mode: 'copy'
    queue 'short'
    memory { 1.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy 'retry'
    maxRetries 3

    input:
    tuple val(GCST), val(status)
    
    output:
    tuple val(GCST), path("total_strand_count.tsv"), env(palin_mode), val("contiune"), emit: all_sum

    when:
    status=="rerun"

    shell:
    """ 
    python ${params.script_path}/bin/sum_strand_counts_nf.py \
    -i ${launchDir}/$GCST/all_sc \
    -o total_strand_count.tsv \
    -t ${params.threshold}

    palin_mode=\$(grep palin_mode total_strand_count.tsv| cut -f2 )
    """
}