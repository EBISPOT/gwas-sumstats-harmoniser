// if reference files are not exist, download and prepare reference
include {get_vcf_files} from '../../modules/local/get_vcf_files'

include {get_variation_tables} from '../../modules/local/get_variation_tables'
include {make_local_synonyms_table} from '../../modules/local/make_local_synonyms_table'

workflow prepare_reference {
    take: in_chrom
    main:
    get_vcf_files(in_chrom)
    
    // output of make_parquet_refs tuple: [chr, vcf, tbi, parquet]

    get_variation_tables(params.remote_ensembl_variation)
    make_local_synonyms_table(get_variation_tables.out.var,get_variation_tables.out.syn)
}