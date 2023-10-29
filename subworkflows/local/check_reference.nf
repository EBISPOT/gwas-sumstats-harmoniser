workflow chr_check {
    
    // check what is available in reference folder
    vcf_files=Channel.fromPath("${params.ref}/*.vcf.gz").map{extract_name(it)}
    tbi_files=Channel.fromPath("${params.ref}/*.tbi").map{extract_name(it)}
    parquet_files=Channel.fromPath("${params.ref}/*.parquet").map{extract_name(it)}

    // check what is required to be harmonized
    // ch_chrom looks like: [chr1,chr2,chr3...]
    ch_chrom=Channel.from(params.chrom).map{"chr"+it}

    //cross check the common chr can run
    ch=vcf_files.join(tbi_files).join(parquet_files).join(ch_chrom)

    if ( !ch ) {
        log.error("ERROR: references are insufficient (please check whether you have vcf.gz, .tbi and .parquet for all your references)")
        System.exit(1)
    }

    ch.toList().subscribe { println it + ' is being harmonized' }

    emit:
    ch_input=ch.collect()
}

def extract_name(Path input){
     return [input.getName().split('-')[1].split('\\.')[0]]
}
