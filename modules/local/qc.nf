process qc {
    publishDir "${launchDir}/$GCST/final", mode: 'copy'
    queue 'short'
    memory { 1.GB * task.attempt }
    time { 1.hour * task.attempt }
    errorStrategy { task.exitStatus in 137..140 ? 'retry' : 'ignore' } // error caused by memory retry; others ignore
    maxRetries 3

    input:
    tuple val(GCST), val(mode), path(all_hm)
    path rsid_sql

    output:
    tuple val(GCST), val(mode),path(all_hm), path('harmonised.qc.tsv'), path('report.txt'), emit: qc_ed
    //path 'harmonised.qc.tsv.gz', emit: qc_result
    //path 'report.txt', emit:delete_sites

    shell:
    """
    python ${params.script_path}/bin/basic_qc_nf.py \
    -f $all_hm \
    -o harmonised.qc.tsv \
    --log report.txt \
    -db $rsid_sql

    (head -n 1 ${launchDir}/$GCST*.tsv| sed 's/^/#/'; sed '1d' ${launchDir}/$GCST*.tsv | sort -k1,1n -k2,2n) | bgzip -c > ${launchDir}/$GCST/final/${GCST}.f.tsv.gz

    (head -n 1 harmonised.qc.tsv| sed 's/^/#/'; sed '1d' harmonised.qc.tsv | sort -k13,13n -k14,14n) | bgzip -c > ${launchDir}/$GCST/final/${GCST}.h.tsv.gz

    tabix -f -s 13 -b 14 -e 14 ${launchDir}/$GCST/final/${GCST}.h.tsv.gz
    """
}

// sort -k1,1n -k2,2n harmonised.qc.tsv | bgzip -c > harmonised.qc.tsv.gz 
// tabix -s 1 -b 2 harmonised.qc.tsv.gz 