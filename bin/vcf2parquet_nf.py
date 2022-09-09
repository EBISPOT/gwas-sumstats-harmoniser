#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import argparse
import pathlib
import os

def process_file(file,outfile):
    vcf_df = pd.read_csv(file, sep="\t", comment="#", usecols=[0,1,2], names=["CHR", "POS", "ID"],engine='python')
    vcf_df.set_index(["ID"])
    extensions = "".join(pathlib.Path(file).suffixes)
    #outfile = file.replace(extensions, ".parquet")
    vcf_df.to_parquet(outfile, index=True)

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The full path to the file to be processed', required=True)
    argparser.add_argument('-o', help='The name of the output file', required=True)
    args = argparser.parse_args()

    file = args.f
    outfile = args.o

    process_file(file,outfile)


if __name__ == "__main__":
    main()
