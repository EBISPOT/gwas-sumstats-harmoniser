# Configure --------------------------------------------------------------------

configfile: "config.yaml"

rule get_vcf_files:
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz", local=config["local_resources"])
    params:
        remote_location=config["remote_vcf_location"],
        local_resources=config["local_resources"]
    shell:
        "wget -P {params.local_resources} {params.remote_location}homo_sapiens-chr{wildcards.chromosome}.vcf.gz"


rule get_tbi_files:
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz.tbi", local=config["local_resources"])
    params:
        remote_location=config["remote_vcf_location"],
        local_resources=config["local_resources"]
    shell:
        "wget -P {params.local_resources} {params.remote_location}/homo_sapiens-chr{wildcards.chromosome}.vcf.gz.tbi"


rule make_parquet_refs:
    input:
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz", local=config["local_resources"])
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.parquet", local=config["local_resources"])
    params:
        local_resources=config["local_resources"]
    shell:
        "python harmoniser/vcf2parquet.py -f {params.local_resources}/homo_sapiens-chr{wildcards.chromosome}.vcf.gz"


rule map_to_build:
    input:
        expand("{local}homo_sapiens-chr{{chromosome}}.parquet", local=config["local_resources"]),
        in_ss="{ss_file}.tsv"
    output:
        "{ss_file}/{chromosome}.merged"
    params:
        local_resources=config["local_resources"],
        to_build=config["desired_build"]
    resources:
        mem_mb = lambda wildcards, attempt: attempt * 28000
    shell:
        "filename={wildcards.ss_file}; "
        "from_build=$(echo -n $filename | tail -c 2); "
        "mkdir -p {wildcards.ss_file}; "
        "python harmoniser/map_to_build.py -f {input.in_ss} "
        "-from_build $from_build "
        "-to_build {params.to_build} "
        "-vcf '{params.local_resources}/homo_sapiens-chr*.parquet'"


rule generate_strand_counts:
    input:
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz", local=config["local_resources"]),
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz.tbi", local=config["local_resources"]),
        in_ss="{ss_file}/{chromosome}.merged"
    output:
        "{ss_file}/{chromosome}.merged.sc"
    params:
        local_resources=config["local_resources"]
    shell:
        "./harmoniser/genetics-sumstat-harmoniser/bin/sumstat_harmoniser --sumstats {input.in_ss} "
        "--vcf {params.local_resources}homo_sapiens-chr{wildcards.chromosome}.vcf.gz "
        "--chrom_col chromosome "
        "--pos_col base_pair_location "
        "--effAl_col effect_allele "
        "--otherAl_col other_allele "
        "--rsid_col variant_id "
        "--strand_counts {input.in_ss}.sc"


rule summarise_strand_counts:
    input:
        expand("{{ss_file}}/{chromosome}.merged.sc", chromosome=config["chromosomes"])
    output:
        "{ss_file}/total_strand_count.tsv"
    shell:
        "python harmoniser/sum_strand_counts.py -i {wildcards.ss_file} -o {wildcards.ss_file} -config config.yaml" 

# harmonise
# cat_chroms
# qc


