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
        #mem_mb = lambda wildcards, attempt: attempt * 28000
        mem_mb = 28000
    shell:
        "filename={wildcards.ss_file}; "
        "from_build=$(echo -n $filename | tail -c 2); "
        "mkdir -p {wildcards.ss_file}; "
        "python harmoniser/map_to_build.py -f {input.in_ss} "
        "-from_build $from_build "
        "-to_build {params.to_build} "
        "-vcf '{params.local_resources}/homo_sapiens-chr*.parquet'"


# split_by_chrom
# strand_counts
# sum_strand_counts
# harmonise
# cat_chroms
# qc


