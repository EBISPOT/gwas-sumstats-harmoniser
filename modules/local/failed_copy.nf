process failed_copy {
    conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:v1.0.1"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:v1.0.1' : dockerimg }"
   

    input:
    tuple val(GCST), path(tsv), path(qc_tsv), path (log), val(status)

    output:
    tuple val(GCST),val(status), env(copy), emit: done

    when:
    status=="FAILED_HARMONIZATION"

    shell:
    """

    log_file=${launchDir}/$GCST/final/${GCST}.running.log
    
    if [[ -f \$log_file ]]
    then
       cp ${launchDir}/$GCST/final/${GCST}.running.log ${params.failed}/
    fi

    copy="copied"
    """
}
