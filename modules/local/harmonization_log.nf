process harmonization_log {

    input:
    path ref
    tuple val(GCST), val(mode), path(all_hm), path(qc_result), path(delete_sites), path(count), val(build), path(input)

    output:
    tuple val(GCST), path(qc_result), path ("${GCST}.running.log"), env(result), emit: running_result

    shell:
    """
    log_script.sh \
    -r $ref \
    -i $input \
    -c $count \
    -d $delete_sites \
    -h $all_hm \
    -o ${GCST}.running.log

    sed 1d $qc_result| awk -F "\t" '{print \$12}' | creat_log.py >> ${GCST}.running.log
    
    result=\$(grep Result ${GCST}.running.log | cut -f2)
    """
}