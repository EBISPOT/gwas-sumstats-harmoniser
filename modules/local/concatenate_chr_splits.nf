process concatenate_chr_splits {
    conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"
   

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
