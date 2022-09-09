process failed_copy {

    input:
    tuple val(GCST), path(tsv), path(qc_tsv), path (log), val(status)

    output:
    tuple val(GCST),val(status), env(copy), emit: done

    when:
    status=="FAILED_HARMONIZATION"

    shell:
    """
    md5_tsv=\$(md5sum $tsv | awk '{print \$1}')

    cp $tsv ${params.failed}/

    log_file=${launchDir}/$GCST/final/${GCST}.running.log
    
    if [[ -f \$log_file ]]
    then
       cp ${launchDir}/$GCST/final/${GCST}.running.log ${params.failed}/
    fi

    md5_tsv_copied=\$(md5sum ${params.failed}/${GCST}*.tsv | awk '{print \$1}')

    if [ \$md5_tsv==\$md5_tsv_copied ]
    then 
         copy="copied"
    else
         copy="failed_copy"
    fi
    """
}