process make_parquet_refs {
    storeDir params.ref
    
    input:
    tuple val(chr), path(vcf), path(tbi)

    output:
    tuple val(chr), path(vcf), path(tbi), path ("homo_sapiens-${chr}.parquet"), emit: ref_processed

    shell:
    """
    python ${params.script_path}/bin/vcf2parquet_nf.py \
    -f ${params.ref}/homo_sapiens-${chr}.vcf.gz \
    -o homo_sapiens-${chr}.parquet
    """
}