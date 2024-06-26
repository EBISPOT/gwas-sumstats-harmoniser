process map_to_build {
    tag "$GCST"

    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"
        
    input:
    tuple val(GCST), path(yaml), path(tsv)
    val chr

    output:
    tuple val(GCST), path ('*.merged'), path('unmapped'), path(yaml), emit:mapped

    shell:
    """
    coordinate_system=\$(grep coordinate_system $yaml | awk -F ":" '{print \$2}' | tr -d "[:blank:]" )
    if test -z "\$coordinate_system"; then coordinate="1-based"; else coordinate=\$coordinate_system; fi
    from_build=\$((grep genome_assembly $yaml | grep -Eo '[0-9][0-9]') || (echo \$(basename $tsv) | grep -Eo '[bB][a-zA-Z]*[0-9][0-9]' | grep -Eo '[0-9][0-9]'))
    [[ -z "\$from_build" ]] && { echo "Parameter from_build is empty" ; exit 1; }

    map_to_build_nf.py \
    -f $tsv \
    -from_build \$from_build \
    -to_build $params.to_build \
    -vcf "${params.ref}/homo_sapiens-chr*.parquet" \
    -chroms "${chr}" \
    -coordinate \$coordinate
    """
}
