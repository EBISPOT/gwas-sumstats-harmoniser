process summarise_strand_counts {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "athenaji/gwas_harm_test"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://athenaji/gwas_harm_test' : dockerimg }"
   

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
