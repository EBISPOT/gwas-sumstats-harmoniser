process map_to_build {

    input:
    tuple val(GCST), val(from_build), path(tsv)
    val chr 

    output:
    tuple val(GCST), path ('*.merged'), emit:mapped

    shell:
    """
    map_to_build_nf.py \
    -f $tsv \
    -from_build $from_build \
    -to_build $params.to_build \
    -vcf "${params.ref}/homo_sapiens-chr*.parquet"
    """
}
