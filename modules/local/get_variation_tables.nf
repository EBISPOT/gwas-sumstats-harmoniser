process get_variation_tables{
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "athenaji/gwas_harm_test"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://athenaji/gwas_harm_test' : dockerimg }"
        
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
