// if reference files are not exist, download and prepare reference
include {get_vcf_files} from '../../modules/local/get_vcf_files'

workflow prepare_reference {
    take: in_chrom
    main:
    
    get_vcf_files(in_chrom)
    
    // output of make_parquet_refs tuple: [chr, vcf, tbi, parquet]
}