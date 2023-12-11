#!/bin/bash

set +u;conda init;set -u
set +u;conda activate ./miniconda3/envs/conda_harmonization/;set -u 

nextflow='path of main.nf'
config='path of nextflow config'
to_harm='path of Ready_to_harmonise'

# Creat a folder as the launch folder (default is using date)
folder=$(date +'%d_%m_%Y')
mkdir -p $folder

cd $folder
nextflow $nextflow -c $config --inputPath $folder --to_harm_folder $to_harm -resume 
#further function: --execute local (or SLURM)
