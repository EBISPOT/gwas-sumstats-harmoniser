process concatenate_chr_splits {
    publishDir "${launchDir}/$GCST/final", mode: 'copy'
    queue 'short'
    memory { 1.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy 'retry'
    maxRetries 3

    input:
    tuple val(GCST), val(palin_mode)

    output:
    tuple val(GCST), val(palin_mode), path ('harmonised.tsv'), emit: all_hm

    shell:
    """
    ${params.script_path}/bin/cat_chroms.sh -d ${launchDir}/$GCST/harmonization -o harmonised.tsv
    """
}
