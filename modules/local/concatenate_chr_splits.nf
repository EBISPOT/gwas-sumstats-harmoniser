process concatenate_chr_splits {
    tag "$GCST"
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"

    input:
    tuple val(GCST), val(palin_mode), path("*")

    output:
    tuple val(GCST), val(palin_mode), path ('harmonised.tsv'), emit: all_hm

    shell:
    """
    head -q -n1 *.merged.hm | head -n1 > harmonised.tsv
    for c in \$(seq 1 22) X Y MT; do
    if [ -f chr\$c.merged.hm ]; then
            echo chr\$c.merged.hm
            tail -n+2 chr\$c.merged.hm >> harmonised.tsv
    fi
    done
    """
}
