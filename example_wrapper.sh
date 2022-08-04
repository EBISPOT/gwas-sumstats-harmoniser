#!/bin/bash

set +u;source ~/.bashrc;set -u
set +u;source /homes/yueji/miniconda3/etc/profile.d/conda.sh;set -u
set +u;conda init;set -u
set +u;conda activate /homes/yueji/miniconda3/envs/conda_harmonization/;set -u 

nextflow='path of main.nf'
config='path of config.ymal'
to_harm='path of Ready_to_harmonise'

# Creat a folder as the launch folder (default is using date)
folder=$(date +'%d_%m_%Y')
mkdir -p $folder

cd $folder
nextflow $nextflow -c $config --inputPath $folder --to_harm $to_harm --date +'%d_%m_%Y' -resume 
