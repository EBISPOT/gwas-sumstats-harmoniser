process ten_percent_counts_sum {
    publishDir "${launchDir}/$GCST", mode: 'copy'
    queue 'short'
    memory { 1.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy 'retry'
    maxRetries 3

    input:
    val GCST
    
    output:
    tuple val(GCST), path("ten_percent_total_strand_count.tsv"), env(mode), env(s), emit: ten_sum

    shell:
    """
    python ${params.script_path}/bin/sum_strand_counts_10percent_nf.py \
    -i ${launchDir}/$GCST/ten_sc \
    -o ten_percent_total_strand_count.tsv \
    -t ${params.threshold} 

    s=\$(cat ten_percent_total_strand_count.tsv | grep status | awk 'BEGIN {FS="\t"}; {print \$2}')
    mode=\$(grep palin_mode ten_percent_total_strand_count.tsv | cut -f2 )
    """
    // config.yaml  provide the threshold of the overall forward or reverse
}