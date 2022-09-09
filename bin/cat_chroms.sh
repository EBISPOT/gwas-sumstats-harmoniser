#!/bin/bash


file_dir=$2
output=$4

head -q -n1 $file_dir/*.merged.hm | head -n1 > $output
for c in $(seq 1 22) X Y MT; do
        if [ -f $file_dir/chr$c.merged.hm ]; then 
        tail -n+2  $file_dir/chr$c.merged.hm >> $output
        fi
done
