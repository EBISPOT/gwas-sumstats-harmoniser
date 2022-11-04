process qc {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "athenaji/gwas_harm_test"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://athenaji/gwas_harm_test' : dockerimg }"
   

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

    (head -n 1 harmonised.qc.tsv; sed '1d' harmonised.qc.tsv | sort -k13,13n -k14,14n) | bgzip -c > ${launchDir}/$GCST/final/${GCST}.h.tsv.gz

    tabix -S 1 -f -s 13 -b 14 -e 14 ${launchDir}/$GCST/final/${GCST}.h.tsv.gz
    """
}
