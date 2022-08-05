process map_to_build {
    publishDir "${launchDir}/$GCST", mode: 'copy'
    queue 'short'
    memory { 40.GB * task.attempt }
    time { 5.hour * task.attempt }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' } // error caused by memory retry; others ignore
    maxRetries 3

    input:
    tuple val(GCST), val(from_build), path(tsv)
    val chr 

    output:
    tuple val(GCST), path ('*.merged'), emit:mapped

    shell:
    """
    mv $tsv ${launchDir}/${GCST}.f.tsv
    
    python ${params.script_path}/bin/map_to_build_nf.py -f ${GCST}.f.tsv \
    -from_build $from_build \
    -to_build $params.to_build \
    -vcf "${params.ref}/homo_sapiens-chr*.parquet"
    """
}
