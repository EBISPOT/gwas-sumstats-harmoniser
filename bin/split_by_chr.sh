#!/bin/bash

file=$1
fname=$(echo $( basename $file | cut -f1 -d '.' ))
pdir=$(dirname $file)

dname="${pdir}/${fname}"
echo "making dir $dname"
mkdir -p $dname

cat $file | tail -n+2 | awk -v n=$dname '{print >> (n "/chr" $1 ".out")}'

for f in $dname/chr*.out; do
        chrom=$(echo $( basename $f | cut -f1 -d '.' ))
        cat $file | head -n1 > "${dname}/${chrom}.tsv"
        cat $f >> "${dname}/${chrom}.tsv"
        rm $f
done
