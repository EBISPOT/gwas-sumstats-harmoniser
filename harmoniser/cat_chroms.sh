#!/bin/bash

file_dir=$2
output=$4


head -q -n1 $file_dir/chr*.merged.hm | head -n1 > $output
for c in {1..22}; do
        tail -n+2  $file_dir/chr$c.merged.hm >> $output
done
tail -n+2  $file_dir/chrX.merged.hm >> $output
tail -n+2  $file_dir/chrY.merged.hm >> $output
tail -n+2  $file_dir/chrMT.merged.hm >> $output
