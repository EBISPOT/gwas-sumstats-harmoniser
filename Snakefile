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
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.parquet", local=config["local_resources"])
    params:
        local_resources=config["local_resources"]
    shell:
        "python harmoniser/vcf2parquet.py -f {params.local_resources}/homo_sapiens-chr{wildcards.chromosome}.vcf.gz"


