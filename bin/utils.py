#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import yaml

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
