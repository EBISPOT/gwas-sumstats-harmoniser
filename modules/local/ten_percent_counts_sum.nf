process ten_percent_counts_sum {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:v1.0.1"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:v1.0.1' : dockerimg }"
   

    input:
    val GCST
    
    output:
    tuple val(GCST), path("ten_percent_total_strand_count.tsv"), env(mode), env(s), emit: ten_sum

    shell:
    """
    sum_strand_counts_10percent_nf.py \
    -i ${launchDir}/$GCST/ten_sc \
    -o ten_percent_total_strand_count.tsv \
    -t ${params.threshold} 

    s=\$(cat ten_percent_total_strand_count.tsv | grep status | awk 'BEGIN {FS="\t"}; {print \$2}')
    mode=\$(grep palin_mode ten_percent_total_strand_count.tsv | cut -f2 )
    """
    // config.yaml  provide the threshold of the overall forward or reverse
}
