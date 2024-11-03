/* download reference */
process get_vcf_files {
  tag "${chr}"
  conda (params.enable_conda ? "${task.ext.conda}" : null)

  container "${ workflow.containerEngine == 'singularity' &&
  !task.ext.singularity_pull_docker_container ?
  "${task.ext.singularity}${task.ext.singularity_version}" :
  "${task.ext.docker}${task.ext.docker_version}" }"

  storeDir params.ref

  input:
    val chr
  output:
    tuple val(chr), path("homo_sapiens-${chr}.parquet"), emit: vcfs

  // Path of output should be the storeDir path
  
  shell:
  """
  # Check if the directory exists; if not, create it
  [[ -d $params.ref ]] || mkdir -p $params.ref

  # Check if the VCF file already exists; if not, download it
  if [[ ! -f $params.ref/homo_sapiens-${chr}.vcf.gz ]]; then
    wget -P $params.ref ${params.remote_vcf_location}/homo_sapiens-${chr}.vcf.gz
  fi

  # Check if the index file exists; if not, create it
  if [[ ! -f $params.ref/homo_sapiens-${chr}.vcf.gz.tbi ]]; then
    tabix -f -p vcf $params.ref/homo_sapiens-${chr}.vcf.gz
  fi

  vcf2parquet_nf.py \
    -f ${params.ref}/homo_sapiens-${chr}.vcf.gz \
    -o homo_sapiens-${chr}.parquet
  """
}
