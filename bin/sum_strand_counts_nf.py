import csv
import sys
import glob
import argparse
import os

from utils import *


def aggregate_counts(in_dir, out, threshold):

    variant_type_dict = {
        "Palindormic variant": 0,
        "Forward strand variant": 0,
        "Reverse strand variant": 0,
        "No VCF record found": 0,
        "Invalid variant for harmonisation": 0
    }

    # decisions based on counts:
    """
    if forward > reverse and (reverse/forward) >= threshold
    """
    all_files = glob.glob(os.path.join(in_dir, "*.sc"))

    with open(os.path.join(out), 'w') as tsvout:
        for f in all_files:
            with open(f,'r') as tsvin:
                tsvin = csv.reader(tsvin, delimiter='\t')
                for line in tsvin:
                    for variant_type, count in variant_type_dict.items():
                        if variant_type == line[0]:
                            variant_type_dict[variant_type] += int(line[1])
 
        for variant_type, count in variant_type_dict.items():
            tsvout.write(variant_type + '\t' + str(count) + '\n')

        fwd = variant_type_dict["Forward strand variant"]
        rev = variant_type_dict["Reverse strand variant"]
        tot = fwd + rev

        if tot > 0:
            tsvout.write("Full_ratio" + "\t"+ str(fwd/tot) + '\n')
            if fwd / tot >= threshold:
                tsvout.write("palin_mode" + "\t" + "forward")
            elif rev / tot >= threshold:
                tsvout.write("palin_mode" + "\t" + "reverse")
            else:
                tsvout.write("palin_mode" + "\t" + "drop")
        else:
            tsvout.write("palin_mode" + "\t" + "drop")


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-i', help='The directory of strand ccount files', required=True)
    argparser.add_argument('-o', help='output file name', required=True)
    argparser.add_argument('-t', help='The threshold determine the number')
    args = argparser.parse_args()
    
    in_dir = args.i
    out = str(args.o)
    threshold = float(args.t)
    aggregate_counts(in_dir,out,threshold)


if __name__ == "__main__":
    main()
