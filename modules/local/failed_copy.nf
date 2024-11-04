process failed_copy {
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    tuple val(GCST), path(raw_yaml), path(tsv), path(htsv), path(tbi), path(running_log), path(yaml), val(status)
    
    output:
    tuple val(GCST),val(status), env(copy), emit: done

    when:
    status=="FAILED_HARMONIZATION"

    shell:
    """
    if [[ $GCST =~ ^GCST[0-9]+ ]]; then
         folder=\$(accession_id.sh -n $GCST)
         path=${params.ftp}/\$folder/$GCST/harmonised/
    else
         path=${params.ftp}/$GCST
     fi

    if [ ! -d \$path ] 
    then
       mkdir -p \$path
    fi
    
    if [[ -f $running_log ]]
    then
       cp $running_log ${params.ftp}/
    fi

    copy="copied"
    rm -vr ${launchDir}/$GCST
    """
}