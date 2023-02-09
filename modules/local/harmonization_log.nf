process harmonization_log {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"
   

    input:
    val chr
    tuple val(GCST), val(mode), path(all_hm), path(qc_result), path(delete_sites), path(count), path(raw_yaml), path(input)
    path(unmapped)

    output:
    tuple val(GCST), path(qc_result), path ("${GCST}.running.log"),  path ("${GCST}.h.tsv.gz-meta.yaml"), env(result), emit: running_result

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

    # metadata file

    dataFileName="${GCST}.h.tsv.gz"
    outYaml="${GCST}.h.tsv.gz-meta.yaml"
    dataFileMd5sum=\$(md5sum<${launchDir}/$GCST/final/${GCST}.h.tsv.gz | awk '{print \$1}')
    dateLastModified=\$(date  +"%Y-%m-%d")
    harmonisationReference=\$(tabix -H "${params.ref}/homo_sapiens-${chr}.vcf.gz" | grep reference | cut -f2 -d '=')
    effectStatistic=\$(zcat ${launchDir}/$GCST/final/${GCST}.h.tsv.gz | head -n1 | cut -f5)

    gwas_metadata.py \
    -i $raw_yaml \
    -o \$outYaml \
    --dataFileName \$dataFileName \
    --dataFileMd5sum \$dataFileMd5sum \
    --isHarmonised True \
    --isSorted True \
    --genomeAssembly GRCh38 \
    --coordinateSystem 1-based \
    --dateLastModified \$dateLastModified \
    --harmonisationReference \$harmonisationReference \
    --effectStatistic \$effectStatistic -e
    """
}