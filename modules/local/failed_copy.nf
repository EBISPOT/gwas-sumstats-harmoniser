process failed_copy {
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    tuple val(GCST), path(qc_tsv), path (log), path (yaml), val(status), path(raw_yaml), path(tsv)

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
    rm -vr ${launchDir}/$GCST
    """
}
