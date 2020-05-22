# merge on hm_variant_id with vcf of desired build
# if variant_id is rsid and ID != variant_id or not synonym --> drop and create discrep df


import pandas as pd
import liftover as lft
from common_constants import *
import os
import glob
import argparse

CHROMOSOMES = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT']

def merge_ss_vcf(ss, vcf, from_build, to_build):
    vcfs = glob.glob(vcf)
    ssdf = pd.read_csv(ss, sep='\t', dtype=str)
    rsid_mask = ssdf[SNP_DSET].str.startswith("rs").fillna(False)
    ssdf_with_rsid = ssdf[rsid_mask]
    ssdf_without_rsid = ssdf[~rsid_mask]
    header = list(ssdf.columns.values)
    dirname = os.path.splitext(ss)[0]
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    while not ssdf_with_rsid.empty:
        for vcf in vcfs:
            vcf_df = pd.read_parquet(vcf)
            chrom = vcf_df.CHR[0]
            mergedf = ssdf_with_rsid.merge(vcf_df, left_on=SNP_DSET, right_on="ID", how="left")
            mapped = mergedf.dropna(subset=["ID"]).drop([CHR_DSET, BP_DSET], axis=1)
            mapped[CHR_DSET] = mapped["CHR"].astype("str").str.replace("\..*$","")
            mapped[BP_DSET] = mapped["POS"].astype("str").str.replace("\..*$","")
            mapped = mapped[header]
            outfile = os.path.join(dirname, "{}.merged".format(chrom))
            mapped.to_csv(outfile, sep="\t", index=False, na_rep="NA")

            ssdf_with_rsid = mergedf[mergedf["ID"].isnull()]
            ssdf_with_rsid = ssdf_with_rsid[header]
    
    ssdf = pd.concat([ssdf_with_rsid, ssdf_without_rsid])
    build_map = lft.LiftOver(lft.ucsc_release.get(from_build), lft.ucsc_release.get(to_build)) if from_build != to_build else None
    if build_map:
        ssdf[BP_DSET] = [lft.map_bp_to_build_via_liftover(chromosome=x, bp=y, build_map=build_map) for x, y in zip(ssdf[CHR_DSET], ssdf[BP_DSET])]
    for chrom in CHROMOSOMES:
        df = ssdf.loc[ssdf[CHR_DSET].astype("str") == chrom]
        df[BP_DSET] = df[BP_DSET].astype("str").str.replace("\..*$","")
        outfile = os.path.join(dirname, "{}.merged".format(chrom))
        if os.path.isfile(outfile):
            df.to_csv(outfile, sep="\t", mode='a', header=False, index=False, na_rep="NA")
        else:
            df.to_csv(outfile, sep="\t", mode='w', index=False, na_rep="NA")
    no_chr_df = ssdf[ssdf[CHR_DSET].isnull()]
    outfile = os.path.join(dirname, "unmapped")
    no_chr_df.to_csv(outfile, sep="\t", index=False, na_rep="NA")



def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-vcf', help='The name of the vcf file', required=True)
    argparser.add_argument('--log', help='The name of the log file')
    argparser.add_argument('-from_build', help='The original build e.g. "36" for NCBI36 or hg18', required=True)
    argparser.add_argument('-to_build', help='The latest (desired) build e.g. "38"', required=True)
    args = argparser.parse_args()

    ss = args.f
    vcf = args.vcf
    from_build = args.from_build
    to_build = args.to_build
    merge_ss_vcf(ss, vcf, from_build, to_build) 



if __name__ == "__main__":
    main()
