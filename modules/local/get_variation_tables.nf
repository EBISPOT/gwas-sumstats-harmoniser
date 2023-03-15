process get_variation_tables{
    conda (params.enable_conda ? "${task.ext.conda}" : null)

    container "${ workflow.containerEngine == 'singularity' &&
        !task.ext.singularity_pull_docker_container ?
        "${task.ext.singularity}${task.ext.singularity_version}" :
        "${task.ext.docker}${task.ext.docker_version}" }"
           
    storeDir params.ref

    input:
    val remote

    output:
    path 'variation.txt.gz', emit: var
    path 'variation_synonym.txt.gz', emit: syn

    shell:
    """
    wget -P $params.ref ${remote}/variation.txt.gz
    wget -P $params.ref ${remote}/variation_synonym.txt.gz
    """
}
