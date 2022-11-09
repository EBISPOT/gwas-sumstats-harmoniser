/* download reference */
process get_vcf_files {
  conda (params.enable_conda ? "$projectDir/environments/conda_environment.yml" : null)
  def dockerimg = "ebispot/gwas-sumstats-harmoniser:latest"
  container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ? 'docker://ebispot/gwas-sumstats-harmoniser:latest' : dockerimg }"
        
  storeDir params.ref
  errorStrategy = { 'ignore' }

  input:
    val chr
  output:
    tuple val(chr), path("homo_sapiens-${chr}.parquet"), emit: vcfs

  // Path of output should be the storeDir path
  
  shell:
  """
  mkdir -p $params.ref
  wget -P $params.ref ${params.remote_vcf_location}/homo_sapiens-${chr}.vcf.gz
  tabix -f -p vcf ${params.ref}/homo_sapiens-${chr}.vcf.gz

  vcf2parquet_nf.py \
    -f ${params.ref}/homo_sapiens-${chr}.vcf.gz \
    -o homo_sapiens-${chr}.parquet
  """
}
