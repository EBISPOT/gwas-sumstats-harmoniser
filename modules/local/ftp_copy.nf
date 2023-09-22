process ftp_copy{
    //conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
    //def dockerimg = "ebispot/gwas-sumstats-harmoniser:v1.0.1"
    //container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:v1.0.1' : dockerimg }"
   

    input:
    tuple val(GCST), path(tsv), path(qc_tsv), path (log), val(status)

    output:
    tuple val(GCST), val(status), env(copy), emit: done

    when:
    status=="SUCCESS_HARMONIZATION"

    shell:
    """
    folder=\$(accession_id.sh -n $GCST)
    path=${params.ftp}/\$folder/$GCST/harmonised/

    if [ ! -d \$path ] 
    then
       mkdir -p \$path
    fi

    cp ${launchDir}/$GCST/final/${GCST}.h.tsv.gz \$path
    md5_h_tsv=\$(md5sum<${launchDir}/$GCST/final/${GCST}.h.tsv.gz | awk '{print \$1}')
    md5_h_tsv_copied=\$(md5sum<\$path/${GCST}.h.tsv.gz | awk '{print \$1}')

    md5_h_tbi=\$(md5sum<${launchDir}/$GCST/final/${GCST}.h.tsv.gz.tbi | awk '{print \$1}')

    cp ${launchDir}/$GCST/final/${GCST}.h.tsv.gz.tbi \$path
    cp ${launchDir}/$GCST/final/${GCST}.running.log  \$path

    if [ \$md5_h_tsv==\$md5_h_tsv_copied ]
    then 
         copy="copied"
         echo "\$md5_h_tsv  ${GCST}.h.tsv.gz" > \$path/md5sum.txt
         echo "\$md5_h_tbi  ${GCST}.h.tsv.gz.tbi" >> \$path/md5sum.txt
         rm -vr ${launchDir}/$GCST
    else
         copy="failed_copy"
    fi
    """
}
