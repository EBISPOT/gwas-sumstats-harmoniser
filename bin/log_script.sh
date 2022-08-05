#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -r reference -c count -d removed -h harmonized -q qc -s script -o output"
   echo -e "\t-r Reference data"
   echo -e "\t-c Total_strand_count"
   echo -e "\t-d Deleted sites in the qc procedure"
   echo -e "\t-h Harmonization result"
   echo -e "\t-q Harmaonization result after qc"
   echo -e "\t-s python script for counting"
   echo -e "\t-o Output file"
   exit 1 # Exit script after printing help
}

while getopts "r:c:d:h:q:s:o:" opt
do
   case "$opt" in
      r ) reference="$OPTARG" ;;
      c ) count="$OPTARG" ;;
      d ) removed="$OPTARG" ;;
      h ) harmonized="$OPTARG" ;;
      q ) qc="$OPTARG" ;;
      s ) script="$OPTARG" ;;
      o ) output="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

echo $reference,$count,$removed,$harmonized,$qc,$script,$output

# Print helpFunction in case parameters are empty
if [ -z "$reference" ] || [ -z "$count" ] || [ -z "$removed" ] || [ -z "$harmonized" ] || [ -z "$qc" ] || [ -z "$script" ] || [ -z "$output" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi

# Begin script in case all parameters are correct
echo -e "1. Pipeline details\n
    A. Pipeline Version: ???\n
    B. Running date: $(date | awk '{print $2,$3,$6}')\n
    C. Input file: $(basename $(dirname $(realpath $harmonized)))\n
" > $output

echo -e "2. Reference data\n
################################################################\n
$(less $reference | head -n 1000 | grep ^# | grep source)\n
$(less $reference | head -n 1000 | grep ^# | grep reference)\n
$(less $reference | head -n 1000 | grep ^# | grep dbSNP | sed 's/INFO=<//g' | sed 's/>//g')\n
################################################################\n
" >> $output

palin_mode=$(grep palin_mode $count | cut -f2);
ratio=$(grep ratio $count);
number=$(echo $ratio | awk '{print $2}')
if [ $palin_mode = "drop" ]; then
if [ ! $number]; then echo -e '3.Palindromic SNPs:\nPalindromic SNPs could not be harmonized because the direction of palindromic SNPs cannot be inferred from consensus direction.\n'>> $output;
else echo -e '3.Palindromic SNPs:\nPalindromic SNPs could not be harmonized because the direction of palindromic SNPs cannot be inferred from consensus direction (forward sites ratio ='$number').\n'>> $output;
fi
elif [[ $ratio =~ "Full" ]]; then
echo -e '3.Palindromic SNPs:\nDirection of palindromic SNPs inferred as '$palin_mode' by establishing consensus direction of all sites (forward sites ratio ='$number').\n'>> $output;
elif [[ $ratio =~ "10_percent" ]]; then
echo -e '3.Palindromic SNPs:\nDirection of palindromic SNPs inferred as '$palin_mode' by establishing consensus direction of 10% of all sites (forward sites ratio ='$number').\n'>> $output;
fi

N=$(wc -l < $harmonized)
n=$(wc -l < $removed)
na=$(wc -l < $qc)
echo -e '4.QC filter:\n'$(awk "BEGIN {print $n/$N*100}")'% ('$n' sites out of '$N') were dropped because p-value = NA, and '$(awk "BEGIN {print $na/$N*100}")'% ('$na' sites) remiain in the final file (.h.tsv.gz).' >> $output

sed 1d $qc| awk -F "\t" '{print $12}' | python $script >> $output
