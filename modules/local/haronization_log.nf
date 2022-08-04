process haronization_log {
    publishDir "${launchDir}/$GCST/final", mode: 'copy'
    queue 'short'
    memory { 1.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy 'retry'
    maxRetries 3

    input:
    path ref
    tuple val(GCST), val(mode), path(all_hm), path(qc_result), path(delete_sites), path(count)

    output:
    tuple val(GCST), path(qc_result), path ("${GCST}.running.log"), env(result), emit: running_result

    shell:
    """
    sh ${params.script_path}/bin/log_script.sh -r $ref -c $count -d $delete_sites -h $all_hm -q $qc_result -s ${params.script_path}/bin/creat_log.py -o ${GCST}.running.log
    result=\$(grep Result ${GCST}.running.log | cut -f2)
    """
}