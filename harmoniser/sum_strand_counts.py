import csv
import sys
import glob
import argparse
import os

from utils import *


def aggregate_counts(in_dir, out_dir, threshold):

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

    with open(os.path.join(out_dir, "total_strand_count.tsv"), 'w') as tsvout:
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
            tsvout.write("Full:ratio" + "\t"+ str(fwd/tot) + '\n')
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
    argparser.add_argument('-o', help='The directory to write to', required=True)
    argparser.add_argument('-config', help='The path to the config.yaml file')
    args = argparser.parse_args()
    
    in_dir = args.i
    out_dir = args.o
    config = args.config
    threshold = get_properties(config)['threshold']
    aggregate_counts(in_dir, out_dir, threshold)


if __name__ == "__main__":
    main()
