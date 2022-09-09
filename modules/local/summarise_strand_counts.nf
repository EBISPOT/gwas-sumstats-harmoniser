process summarise_strand_counts {

    input:
    tuple val(GCST), val(status)
    
    output:
    tuple val(GCST), path("total_strand_count.tsv"), env(palin_mode), val("contiune"), emit: all_sum

    when:
    status=="rerun"

    shell:
    """ 
    sum_strand_counts_nf.py \
    -i ${launchDir}/$GCST/all_sc \
    -o total_strand_count.tsv \
    -t ${params.threshold}

    palin_mode=\$(grep palin_mode total_strand_count.tsv| cut -f2 )
    """
}