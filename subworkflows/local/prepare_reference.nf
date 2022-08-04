// if reference files are not exist, download and prepare reference
include {get_vcf_files} from '../../modules/local/get_vcf_files'
include {get_tbi_files} from '../../modules/local/get_tbi_files'
include {make_parquet_refs} from '../../modules/local/make_parquet_refs'

include {get_variation_tables} from '../../modules/local/get_variation_tables'
include {make_local_synonyms_table} from '../../modules/local/make_local_synonyms_table'

workflow prepare_reference {
    take: in_chrom
    main:
    get_vcf_files(in_chrom)
    get_tbi_files(get_vcf_files.out.vcfs)
    make_parquet_refs(get_tbi_files.out.tbi)
    // output of make_parquet_refs tuple: [chr, vcf, tbi, parquet]

    get_variation_tables(params.remote_ensembl_variation)
    make_local_synonyms_table(get_variation_tables.out.var,get_variation_tables.out.syn)

    emit:
    parquet=make_parquet_refs.out.ref_processed // channel contains reference path
    tbl=make_local_synonyms_table.out.sql // channel contains path of sql

}