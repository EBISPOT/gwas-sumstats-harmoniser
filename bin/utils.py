import csv
import sys
import yaml
import pandas as pd
import argparse
import subprocess
from ast import literal_eval


sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


def get_chromosome_list(config_path):
    return get_properties(config_path)['chromosomes']

    
def get_properties(config_path):
    with open(config_path, 'r') as stream:
        configs = yaml.load(stream, Loader=yaml.FullLoader)
        return configs


def get_csv_reader(csv_file):
    dialect = csv.Sniffer().sniff(csv_file.readline())
    csv_file.seek(0)
    return csv.reader(csv_file, dialect)


def get_filename(file):
    return file.split("/")[-1].split(".")[0]


def list_headers(csv_file):
    header_list = pd.read_table(csv_file, nrows=1).columns.values.tolist()
    return header_list

def get_harmonisation_mapper_args(csv_file):
    arg_list = ["{v} {k}".format(k=k, v=v) for k, v in HARMONISER_ARG_MAP.items() if k in list_headers(csv_file)]
    harm_args = " ".join(arg_list)
    return harm_args

def get_nextflow_config():
    nf_conf_str = subprocess.run(['nextflow', 'config', '-flat'], stdout=subprocess.PIPE).stdout.decode('utf-8')
    nf_conf_list = nf_conf_str.split("\n")
    nf_conf = {}
    for p in nf_conf_list:
        if "=" in p:
            k, v = p.split("=")
            k = k.strip()
            v = v.strip()
            if v.startswith("[") and v.endswith("]"):
                v = literal_eval(v)
            nf_conf[k] = v
    return nf_conf


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-harm_args',
                           help='Return the header mapper arguments for the harmonisation',
                           action='store_true')
    args = argparser.parse_args()

    ss_file = args.f
    harm_args = args.harm_args

    if harm_args:
        print(get_harmonisation_mapper_args(ss_file))


if __name__ == '__main__':
    main()