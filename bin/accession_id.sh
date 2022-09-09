#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -n GCST"
   echo -e "\t-n input GCST number"
   exit 1 # Exit script after printing help
}

while getopts "n:" opt
do
   case "$opt" in
      n ) GCST="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$GCST" ]
then
   echo "GCST number is empty";
   helpFunction
fi

id=$(echo $GCST|sed 's/GCST/1/g')
lower=$((10#$id/1000*1000+1))
upper=$(((10#$id/1000+1)*1000))

gl=GCST${lower:1}
gu=GCST${upper:1}

echo $gl-$gu
