process qc {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e' : dockerimg }"
   

    input:
    tuple val(GCST), val(mode), path(all_hm)

    output:
    tuple val(GCST), val(mode),path(all_hm), path('harmonised.qc.tsv'), path('report.txt'), emit: qc_ed
    //path 'harmonised.qc.tsv.gz', emit: qc_result
    //path 'report.txt', emit:delete_sites

    shell:
    """
    basic_qc_nf.py \
    -f $all_hm \
    -o harmonised.qc.tsv \
    --log report.txt \
    -db ${params.ref}/rsID.sql

    cat harmonised.qc.tsv | bgzip -c > ${launchDir}/$GCST/final/${GCST}.h.tsv.gz

    tabix -c N -S 1 -f -s 3 -b 4 -e 4 ${launchDir}/$GCST/final/${GCST}.h.tsv.gz
    """
}
