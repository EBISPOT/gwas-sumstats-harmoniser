process concatenate_chr_splits {

    input:
    tuple val(GCST), val(palin_mode)

    output:
    tuple val(GCST), val(palin_mode), path ('harmonised.tsv'), emit: all_hm

    shell:
    """
    cat_chroms.sh -d ${launchDir}/$GCST/harmonization -o harmonised.tsv
    """
}
