process qc {
    tag "$GCST"
    
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

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

    chr=\$(awk -v RS='\t' '/chromosome/{print NR; exit}' harmonised.qc.tsv)
    pos=\$(awk -v RS='\t' '/base_pair_location/{print NR; exit}' harmonised.qc.tsv)

    cat harmonised.qc.tsv | bgzip -c > ${launchDir}/$GCST/final/${GCST}.h.tsv.gz
    tabix -c N -S 1 -f -s \$chr -b \$pos -e \$pos ${launchDir}/$GCST/final/${GCST}.h.tsv.gz
    """
}
