process ten_percent_counts {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "ebispot/gwas-sumstats-harmoniser:v1.0.1"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:v1.0.1' : dockerimg }"
   

    input:
    tuple val(chrom), val(GCST), path(merged), path(ref) 

    output:
    tuple val(GCST), path("ten_percent_${chrom}.sc"), emit: ten_sc

    shell:
    """
    select=\$[\$(wc -l < $merged)]

    if [ \$[\$select/10] -gt 100 ]
    then n=\$[\$select/10]
    else n=\$select
    fi

    (head -n 1 $merged; sed '1d' $merged| shuf -n \$n)>ten_percent.${chrom}.merged

    main_pysam.py \
    --sumstats ten_percent.${chrom}.merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    --chrom_col chromosome \
    --pos_col base_pair_location \
    --effAl_col effect_allele \
    --otherAl_col other_allele \
    --rsid_col variant_id \
    --strand_counts ten_percent_${chrom}.sc
    """
}
