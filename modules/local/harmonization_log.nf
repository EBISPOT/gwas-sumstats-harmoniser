process harmonization_log {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"
   

    input:
    val chr
    tuple val(GCST), val(mode), path(all_hm), path(qc_result), path(delete_sites), path(count), val(build), path(input)
    path(unmapped)

    output:
    tuple val(GCST), path(qc_result), path ("${GCST}.running.log"), env(result), emit: running_result

    shell:
    """
    log_script.sh \
    -r "${params.ref}/homo_sapiens-${chr}.vcf.gz" \
    -i $input \
    -c $count \
    -d $delete_sites \
    -h $all_hm \
    -u $unmapped \
    -o ${GCST}.running.log

    N=\$(awk -v RS='\t' '/hm_code/{print NR; exit}' $qc_result)
    sed 1d $qc_result| awk -F "\t" '{print \$'"\$N"'}' | creat_log.py >> ${GCST}.running.log
    
    result=\$(grep Result ${GCST}.running.log | cut -f2)
    """
}
