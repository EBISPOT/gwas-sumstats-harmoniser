#!/bin/bash

helpFunction()
{
   echo -e ""
   echo -e "Usage: $0 -r reference -i input -c count -d removed -h harmonized -u unmapped -q qc -s script -o output\n"
   echo -e "\t-r Reference data\n"
   echo -e "\t-i input raw data\n"
   echo -e "\t-c Total_strand_count\n"
   echo -e "\t-d Deleted sites in the qc procedure\n"
   echo -e "\t-h Harmonization result\n"
   echo -e "\t-u unmapped sites file\n"
   echo -e "\t-o Output file\n"
   exit 1 # Exit script after printing help
}

while getopts "r:i:c:d:h:u:o:" opt
do
   case "$opt" in
      r ) reference="$OPTARG" ;;
      i ) input="$OPTARG" ;;
      c ) count="$OPTARG" ;;
      d ) removed="$OPTARG" ;;
      h ) harmonized="$OPTARG" ;;
      u ) unmapped="$OPTARG" ;;
      o ) output="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

echo -e $reference,$input,$count,$removed,$harmonized,$unmapped,$qc,$script,$output

# Print helpFunction in case parameters are empty
if [ -z "$reference" ] || [ -z "$input" ] || [ -z "$count" ] || [ -z "$removed" ] || [ -z "$harmonized" ] || [ -z "$unmapped" ] || [ -z "$output" ]
then
   echo -e "Some or all of the parameters are empty";
   helpFunction
fi

# Begin script in case all parameters are correct

# PIPELINE META

echo -e "################################################################\n
HARMONISATION RUNNING REPORT\n
################################################################\n\n
" > $output

echo -e "
1. Pipeline details\n
    A. Pipeline Version: 0.1.0\n
    B. Running date: $(date | awk '{print $2,$3,$6}')\n
    C. Input file: $(basename $input)\n
################################################################\n\n
" >> $output

# REFERENCE

echo -e "
2. Reference data\n
$(tabix -H $reference | grep source)\n
$(tabix -H $reference | grep reference)\n
$(tabix -H $reference | grep dbSNP | sed 's/INFO=<//g' | sed 's/>//g')\n
################################################################\n\n
" >> $output

# MAPPING

UNMAPPED_SITES=$(tail -n+2 $unmapped | wc -l)
MAPPED_SITES=$(tail -n+2 $harmonized | wc -l)
TOTAL_SITES=$(($UNMAPPED_SITES + $MAPPED_SITES))

#TODO: add the number of rs vs liftover
echo -e '
3. Mapping result\n\n'$(awk "BEGIN {print $UNMAPPED_SITES/$TOTAL_SITES*100}")'% ('$UNMAPPED_SITES' sites out of '$TOTAL_SITES') were dropped because they could not be mapped. \n'$(awk "BEGIN {print $MAPPED_SITES/$TOTAL_SITES*100}")'% ('$MAPPED_SITES' sites) were carried forward.\n

################################################################\n\n
' >> $output

# PALIN MODE

palin_mode=$(grep palin_mode $count | cut -f2);
echo -e '4. Palindromic SNPs\n\npalin_mode: '$palin_mode'\n' >> $output
ratio=$(grep ratio $count);
number=$(echo -e $ratio | awk '{print $2}')
if [ $palin_mode = "drop" ]; then
if [ ! $number]; then echo -e 'Palindromic SNPs could not be harmonized because the direction of palindromic SNPs cannot be inferred from consensus direction.\n'>> $output;
else echo -e 'Palindromic SNPs could not be harmonized because the direction of palindromic SNPs cannot be inferred from consensus direction (forward sites ratio ='$number').\n'>> $output;
fi
elif [[ $ratio =~ "Full" ]]; then
echo -e 'Direction of palindromic SNPs inferred as '$palin_mode' by establishing consensus direction of all sites (forward sites ratio ='$number').\n'>> $output;
elif [[ $ratio =~ "10_percent" ]]; then
echo -e 'Direction of palindromic SNPs inferred as '$palin_mode' by establishing consensus direction of 10% of all sites (forward sites ratio ='$number').\n'>> $output;
fi

echo -e '################################################################\n\n' >> $output

