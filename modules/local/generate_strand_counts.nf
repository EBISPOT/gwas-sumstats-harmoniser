process generate_strand_counts {
    conda (params.enable_conda ? "$projectDir/environments/pgscatalog_utils/environment.yml" : null)
    def dockerimg = "athenaji/gwas_harm_test"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://athenaji/gwas_harm_test' : dockerimg }"
   
    input:
    tuple val(GCST), val(chrom), path(merged), path(vcf), val(status)

    output:
    tuple val(GCST), val(status), path("full_${chrom}.sc"), emit: all_sc

    when:
    status=="rerun"

    shell:
    """
    main_pysam.py \
    --sumstats $merged \
    --vcf ${params.ref}/homo_sapiens-${chrom}.vcf.gz \
    --chrom_col chromosome \
    --pos_col base_pair_location \
    --effAl_col effect_allele \
    --otherAl_col other_allele \
    --rsid_col variant_id \
    --strand_counts full_${chrom}.sc
    """
}
