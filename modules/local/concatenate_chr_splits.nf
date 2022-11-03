process concatenate_chr_splits {
    conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e' : dockerimg }"
   

    input:
    tuple val(GCST), val(palin_mode), path(hm)

    output:
    tuple val(GCST), val(palin_mode), path ('harmonised.tsv'), emit: all_hm

    shell:
    """
    cat_chroms.sh -d ${launchDir}/$GCST/harmonization -o harmonised.tsv
    """
}
