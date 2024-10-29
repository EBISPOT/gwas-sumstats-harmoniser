process update_meta_yaml {
    tag "$GCST"
    
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    tuple val(chr), val(GCST), path(raw_yaml), path(zip_harm) , path(zip_harm_tbi), path (running_log), env(result)
    
    output:
    tuple val(GCST), path(zip_harm) , path(zip_harm_tbi), path (running_log), path ("${GCST}.h.tsv.gz-meta.yaml"), env(result), emit: running_result

    shell:
    """
    # metadata file

    data_file_name="${GCST}.h.tsv.gz"
    out_yaml="${GCST}.h.tsv.gz-meta.yaml"
    data_file_md5sum=\$(md5sum<$zip_harm | awk '{print \$1}')
    date_metadata_last_modified=\$(date  +"%Y-%m-%d")
    harmonisation_reference=\$(tabix -H "${params.ref}/homo_sapiens-${chr}.vcf.gz" | grep reference | cut -f2 -d '=')

    gwas_metadata.py \
    -i $raw_yaml \
    -o \$out_yaml \
    -e \
    --data_file_name \$data_file_name \
    --data_file_md5sum \$data_file_md5sum \
    --is_harmonised True \
    --is_sorted True \
    --genome_assembly GRCh38 \
    --coordinate_system 1-based \
    --date_metadata_last_modified \$date_metadata_last_modified \
    --harmonisation_reference \$harmonisation_reference \
    """
}