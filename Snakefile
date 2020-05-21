# Configure --------------------------------------------------------------------

configfile: "config.yaml"

rule get_vcf_files:
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz", local=config["vcf_local_path"])
    params:
        remote_location=config["remote_vcf_location"],
        local_vcf=config["vcf_local_path"]
    shell:
        "wget -P {params.local_vcf} {params.remote_location}homo_sapiens-chr{wildcards.chromosome}.vcf.gz"


rule get_tbi_files:
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.vcf.gz.tbi", local=config["vcf_local_path"])
    params:
        remote_location=config["remote_vcf_location"],
        local_vcf=config["vcf_local_path"]
    shell:
        "wget -P {params.local_vcf} {params.remote_location}/homo_sapiens-chr{wildcards.chromosome}.vcf.gz.tbi"

rule make_parquet_refs:
    output:
        expand("{local}homo_sapiens-chr{{chromosome}}.parquet", local=config["vcf_local_path"])
    params:
        remote_location=config["remote_vcf_location"],
        local_vcf=config["vcf_local_path"]
    shell:
        "wget -P {params.local_vcf} {params.remote_location}homo_sapiens-chr{wildcards.chromosome}.vcf.gz"


