process get_tbi_files {
  storeDir params.ref
  
  input:
  tuple val(chr), path(vcf)

  output:
  tuple val(chr), path(vcf), path("homo_sapiens-${chr}.vcf.gz.tbi"), emit:tbi
  
  // Path of output should be the storeDir path

  shell:
  """
  tabix -p vcf $vcf
  """
}