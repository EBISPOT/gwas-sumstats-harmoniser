#!/bin/bash


file_dir=$1
cat $file_dir/1.merged.hm > $file_dir/all.merged.hm
for c in {2..22}; do
        tail -n+2  $file_dir/$c.merged.hm >> $file_dir/all.merged.hm
done
tail -n+2  $file_dir/X.merged.hm >> $file_dir/all.merged.hm
tail -n+2  $file_dir/Y.merged.hm >> $file_dir/all.merged.hm
tail -n+2  $file_dir/MT.merged.hm >> $file_dir/all.merged.hm
