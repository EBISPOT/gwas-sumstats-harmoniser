process concatenate_chr_splits {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "athenaji/gwas_harm_test"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://athenaji/gwas_harm_test' : dockerimg }"
   

    input:
    tuple val(GCST), val(palin_mode)

    output:
    tuple val(GCST), val(palin_mode), path ('harmonised.tsv'), emit: all_hm

    shell:
    """
    cat_chroms.sh -d ${launchDir}/$GCST/harmonization -o harmonised.tsv
    """
}
