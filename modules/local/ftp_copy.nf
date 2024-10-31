process ftp_copy{
    tag "$GCST"
    //conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
    //def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
    //container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"
   

    input:
    tuple val(GCST), path(raw_yaml), path(tsv), path(htsv), path(tbi), path(running_log), path(yaml), val(status)
    //[GCST1, yaml,tsv,h.tsv.gz,h.tsv.gz.tbi, running.log,h.tsv.gz-meta.yaml, SUCCESS_HARMONIZATION]
    output:
    tuple val(GCST), val(status), env(copy), emit: done

    when:
    status=="SUCCESS_HARMONIZATION"

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

    cp $htsv \$path
    md5_h_tsv=\$(md5sum<${launchDir}/$GCST/final/${GCST}.h.tsv.gz | awk '{print \$1}')
    md5_h_tsv_copied=\$(md5sum<\$path/${GCST}.h.tsv.gz | awk '{print \$1}')

    md5_h_tbi=\$(md5sum<${launchDir}/$GCST/final/${GCST}.h.tsv.gz.tbi | awk '{print \$1}')

    cp $tbi \$path
    cp $running_log  \$path
    cp $yaml  \$path

    if [ \$md5_h_tsv==\$md5_h_tsv_copied ]
    then 
         copy="copied"
         echo "\$md5_h_tsv  ${GCST}.h.tsv.gz" > \$path/md5sum.txt
         echo "\$md5_h_tbi  ${GCST}.h.tsv.gz.tbi" >> \$path/md5sum.txt
         rm -v ${params.all_harm_folder}/$tsv
         rm -v ${params.all_harm_folder}/$raw_yaml
         rm -vr ${launchDir}/$GCST
    else
         copy="failed_copy"
    fi
    """
}
