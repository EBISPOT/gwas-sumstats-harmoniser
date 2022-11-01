process harmonization_log {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e' : dockerimg }"
   

    input:
    val chr
    tuple val(GCST), val(mode), path(all_hm), path(qc_result), path(delete_sites), path(count), val(build), path(input)

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
    -o ${GCST}.running.log

    sed 1d $qc_result| awk -F "\t" '{print \$12}' | creat_log.py >> ${GCST}.running.log
    
    result=\$(grep Result ${GCST}.running.log | cut -f2)
    """
}
