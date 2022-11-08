process harmonization {

    conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:6472eaf3b58d76efe01327f74da9b8dc4eb8920e' : dockerimg }"
   
        
    input:
    tuple val(GCST), val(palin_mode), val(status), val(chrom), path(merged), path(ref)


    output:
    tuple val(GCST), val(palin_mode), path("${chrom}.merged.hm"),path("${chrom}.merged.log.tsv.gz"), emit: hm_by_chrom

    when:
    status=="contiune"

    shell:
    """
    header_args=\$(utils.py -f $merged -harm_args);
    main_pysam.py \
    --sumstats $merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    --hm_sumstats ${chrom}.merged_unsorted.hm \
    --hm_statfile ${chrom}.merged.log.tsv.gz \
    \$header_args \
    --na_rep_in NA \
    --na_rep_out NA \
    --palin_mode $palin_mode;

    head -n1 ${chrom}.merged_unsorted.hm > ${chrom}.merged.hm;
    tail -n+2 ${chrom}.merged_unsorted.hm | sort -k3,3n -k4,4n >> ${chrom}.merged.hm
    """
}
