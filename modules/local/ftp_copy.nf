process ftp_copy{
    publishDir "${params.ftp}", mode: 'move'
    queue 'short' // will change into datamover in real case.
    memory { 1.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy 'retry'
    maxRetries 3

    input:
    tuple val(GCST), path(tsv), path(qc_tsv), path (log), val(status)

    output:
    tuple val(GCST), val(status), env(copy), emit: done

    when:
    status=="SUCCESS_HARMONIZATION"

    shell:
    """
    folder=\$(sh ${params.script_path}/bin/accession_id.sh -n $GCST)
    path=${params.ftp}/\$folder/$GCST/harmonised/

    if [ ! -d \$path ] 
    then
       mkdir -p \$path
    fi

    cp ${launchDir}/$GCST/final/${GCST}.f.tsv.gz \$path

    md5_f_tsv=\$(md5sum<${launchDir}/$GCST/final/${GCST}.f.tsv.gz | awk '{print \$1}')
    md5_f_tsv_copied=\$(md5sum<\$path/${GCST}.f.tsv.gz | awk '{print \$1}')
    

    cp ${launchDir}/$GCST/final/${GCST}.h.tsv.gz \$path
    md5_h_tsv=\$(md5sum<${launchDir}/$GCST/final/${GCST}.h.tsv.gz | awk '{print \$1}')
    md5_h_tsv_copied=\$(md5sum<\$path/${GCST}.h.tsv.gz | awk '{print \$1}')

    cp ${launchDir}/$GCST/final/${GCST}.h.tsv.gz.tbi \$path
    cp ${launchDir}/$GCST/final/${GCST}.running.log  \$path

    if [ \$md5_f_tsv==\$md5_f_tsv_copied ] && [ \$md5_h_tsv==\$md5_h_tsv_copied ]
    then 
         copy="copied"
    else
         copy="failed_copy"
    fi
    """
}