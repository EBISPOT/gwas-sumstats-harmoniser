#!/bin/bash

snakemake_dir=./
source .env/bin/activate
cd $snakemake_dir

for f in /toharmonise/*.tsv; do
    n=$(echo $f | sed "s/.tsv//g")
    h=$n/harmonised.qc.tsv
    # if job already running or snakemake target already exists.
    if bjobs -w | grep -q $n || [ -e $h ] ; then
        :
    else
        echo "Submitting $n for harmonisation"
        mkdir -p $n
        bsub "snakemake -d $n --configfile $snakemake_dir/config.yaml --profile lsf $h"
    fi
done
