import pandas as pd
import argparse
import os

def process_file(file):
    vcf_df = pd.read_csv(file, sep="\t", comment="#", usecols=[0,1,2], names=["CHR", "POS", "ID"])
    vcf_df.set_index(["ID"])
    outfile = file.split("/")[-1].split(".")[0]
    vcf_df.to_parquet('{}.parquet'.format(outfile), index=True)

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The full path to the file to be processed', required=True)
    args = argparser.parse_args()

    file = args.f

    process_file(file)


if __name__ == "__main__":
    main()
