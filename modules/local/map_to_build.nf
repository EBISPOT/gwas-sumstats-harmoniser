process map_to_build {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"

    input:
    tuple val(GCST), path(yaml), path(tsv)
    val chr

    output:
    tuple val(GCST), path ('*.merged'), path('unmapped'), emit:mapped

    shell:
    """
    from_build=\$(grep genomeAssembly $yaml | awk -F "GRCh" '{print $2}' )
    map_to_build_nf.py \
    -f $tsv \
    -from_build \$from_build \
    -to_build $params.to_build \
    -vcf "${params.ref}/homo_sapiens-chr*.parquet" \
    -chroms "${chr}"
    """
}
