process harmonization_log {
    tag "$GCST"
    
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    val chr
    tuple val(GCST), val(mode), path(all_hm), path(qc_result), path(delete_sites), path(count), path(raw_yaml), path(input), path(unmapped)

    output:
    tuple val(chr), val(GCST), path(raw_yaml), path("${GCST}.h.tsv.gz"), path("${GCST}.h.tsv.gz.tbi"), path(qc_result), path ("${GCST}.running.log"), env(result)

    shell:
    """
    # Generating running log
    log_script.sh \
    -r "${params.ref}/homo_sapiens-${chr}.vcf.gz" \
    -i $input \
    -c $count \
    -d $delete_sites \
    -h $all_hm \
    -u $unmapped \
    -o ${GCST}.running.log \
    -p ${params.version}

    N=\$(awk -v RS='\t' '/hm_code/{print NR; exit}' $qc_result)
    sed 1d $qc_result| awk -F "\t" '{print \$'"\$N"'}' | creat_log.py >> ${GCST}.running.log
    
    # extract harmonise result
    result=\$(grep Result ${GCST}.running.log | cut -f2)

    # Prepare the gzip data
    chr=\$(awk -v RS='\t' '/chromosome/{print NR; exit}' $qc_result)
    pos=\$(awk -v RS='\t' '/base_pair_location/{print NR; exit}' $qc_result)

    cat $qc_result | bgzip -c > ${GCST}.h.tsv.gz
    tabix -c N -S 1 -f -s \$chr -b \$pos -e \$pos ${GCST}.h.tsv.gz
    """
}