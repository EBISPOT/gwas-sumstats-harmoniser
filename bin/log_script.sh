#!/bin/bash

helpFunction()
{
   printf ""
   printf "Usage: $0 -r reference -i input -c count -d removed -h harmonized -q qc -s script -o output\n"
   printf "\t-r Reference data\n"
   printf "\t-i input raw data\n"
   printf "\t-c Total_strand_count\n"
   printf "\t-d Deleted sites in the qc procedure\n"
   printf "\t-h Harmonization result\n"
   printf "\t-o Output file\n"
   exit 1 # Exit script after printing help
}

while getopts "r:i:c:d:h:o:" opt
do
   case "$opt" in
      r ) reference="$OPTARG" ;;
      i ) input="$OPTARG" ;;
      c ) count="$OPTARG" ;;
      d ) removed="$OPTARG" ;;
      h ) harmonized="$OPTARG" ;;
      o ) output="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

echo $reference,$input,$count,$removed,$harmonized,$qc,$script,$output

# Print helpFunction in case parameters are empty
if [ -z "$reference" ] || [ -z "$input" ] || [ -z "$count" ] || [ -z "$removed" ] || [ -z "$harmonized" ] || [ -z "$output" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi

# Begin script in case all parameters are correct
printf "1. Pipeline details\n
    A. Pipeline Version: ???\n
    B. Running date: $(date | awk '{print $2,$3,$6}')\n
    C. Input file: $(basename $input)\n
" > $output

printf "2. Reference data\n
################################################################\n
$(zcat $reference | head -n 1000 | grep ^# | grep source)\n
$(zcat $reference | head -n 1000 | grep ^# | grep reference)\n
$(zcat $reference | head -n 1000 | grep ^# | grep dbSNP | sed 's/INFO=<//g' | sed 's/>//g')\n
################################################################\n
" >> $output

palin_mode=$(grep palin_mode $count | cut -f2);
ratio=$(grep ratio $count);
number=$(echo $ratio | awk '{print $2}')
if [ $palin_mode = "drop" ]; then
if [ ! $number]; then printf '3.Palindromic SNPs:\nPalindromic SNPs could not be harmonized because the direction of palindromic SNPs cannot be inferred from consensus direction.\n'>> $output;
else printf '3.Palindromic SNPs:\nPalindromic SNPs could not be harmonized because the direction of palindromic SNPs cannot be inferred from consensus direction (forward sites ratio ='$number').\n'>> $output;
fi
elif [[ $ratio =~ "Full" ]]; then
printf '3.Palindromic SNPs:\nDirection of palindromic SNPs inferred as '$palin_mode' by establishing consensus direction of all sites (forward sites ratio ='$number').\n'>> $output;
elif [[ $ratio =~ "10_percent" ]]; then
printf '3.Palindromic SNPs:\nDirection of palindromic SNPs inferred as '$palin_mode' by establishing consensus direction of 10% of all sites (forward sites ratio ='$number').\n'>> $output;
fi

N=$(wc -l < $harmonized)
n=$(wc -l < $removed)
na=$(wc -l < $qc)
printf '4.QC filter:\n'$(awk "BEGIN {print $n/$N*100}")'% ('$n' sites out of '$N') were dropped because p-value = NA, and '$(awk "BEGIN {print $na/$N*100}")'% ('$na' sites) remiain in the final file (.h.tsv.gz).\n' >> $output
