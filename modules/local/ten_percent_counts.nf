
process ten_percent_counts {
    tag "${GCST}_${chrom}"

    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    tuple val(chrom), val(GCST), path(merged), path(yaml), path(ref) 

    output:
    tuple val(GCST), path("ten_percent_${chrom}.sc"), emit: ten_sc

    shell:
    """
    select=\$[\$(wc -l < $merged)]

    if [ \$[\$select/10] -gt 100 ]
    then n=\$[\$select/10]
    else n=\$select
    fi

    (head -n 1 $merged; sed '1d' $merged| shuf -n \$n)>ten_percent.${chrom}.merged

    header_args=\$(utils.py -f $merged -strand_count_args);
    coordinate_system=\$(grep coordinate_system $yaml | awk -F ":" '{print \$2}' | tr -d "[:blank:]" )
    if test -z "\$coordinate_system"; then coordinate="1-based"; else coordinate=\$coordinate_system; fi

    main_pysam.py \
    --sumstats ten_percent.${chrom}.merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    \$header_args \
    --strand_counts ten_percent_${chrom}.sc \
    --coordinate \$coordinate
    """
}
