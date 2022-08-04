/* download reference */
process get_vcf_files {
  storeDir params.ref

  input:
    val chr
  output:
    tuple val(chr), path("homo_sapiens-${chr}.vcf.gz"), emit: vcfs

  // Path of output should be the storeDir path
  
  shell:
  """
  mkdir -p $params.ref
  wget -P $params.ref ${params.remote_vcf_location}/homo_sapiens-${chr}.vcf.gz
  """
}