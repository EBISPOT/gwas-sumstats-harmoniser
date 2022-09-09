process make_local_synonyms_table{
    storeDir params.ref

    input:
    path var
    path syn

    output:
    path 'rsID.sql', emit: sql

    shell:
    """
    make_synonym_table.py -f $var -id_col 0 -name_col 2 -db ${params.ref}/rsID.sql
    makes_synonym_table.py -f $syn -id_col 1 -name_col 4 -db ${params.ref}/rsID.sql
    make_synonym_table.py -index -db ${params.ref}/rsID.sql
    """
}