process get_variation_tables{
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