process map_to_build {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e' : dockerimg }"

    input:
    tuple val(GCST), val(from_build), path(tsv)
    val chr

    output:
    tuple val(GCST), path ('*.merged'), path('unmapped'), emit:mapped

    shell:
    """
    map_to_build_nf.py \
    -f $tsv \
    -from_build $from_build \
    -to_build $params.to_build \
    -vcf "${params.ref}/homo_sapiens-chr*.parquet" \
    -chroms "${chr}"
    """
}
