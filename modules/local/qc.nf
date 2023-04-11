process qc {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"
   

    input:
    tuple val(GCST), val(mode), path(all_hm)
    path(rsIDfile)    

    output:
    tuple val(GCST), val(mode),path(all_hm), path('harmonised.qc.tsv'), path('report.txt'), emit: qc_ed
    //path 'harmonised.qc.tsv.gz', emit: qc_result
    //path 'report.txt', emit:delete_sites
    //tuple path("${GCST}.h.tsv.gz"), path("${GCST}.h.tsv.gz.tbi"), emit: ftp_folder_results

    shell:
    """
    basic_qc_nf.py \
    -f $all_hm \
    -o harmonised.qc.tsv \
    --log report.txt \
    -db $rsIDfile

    cat harmonised.qc.tsv | bgzip -c > ${GCST}.h.tsv.gz
    
    tabix -c N -S 1 -f -s 3 -b 4 -e 4 ${GCST}.h.tsv.gz
    """
}
