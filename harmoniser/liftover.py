import csv
import argparse
from pyliftover import LiftOver

from utils import *
from ensembl_rest_client import EnsemblRestClient


ucsc_release = {'28': 'hg10',
                '29': 'hg11',
                '30': 'hg13',
                '31': 'hg14',
                '33': 'hg15',
                '34': 'hg16',
                '35': 'hg17',
                '36': 'hg18',
                '37': 'hg19',
                '38': 'hg38'}


suffixed_release = {'28': 'NCBI28',
                    '29': 'NCBI29',
                    '30': 'NCBI30',
                    '31': 'NCBI31',
                    '33': 'NCBI33',
                    '34': 'NCBI34',
                    '35': 'NCBI35',
                    '36': 'NCBI36',
                    '37': 'GRCh37',
                    '38': 'GRCh38'}


def isNumber(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def map_bp_to_build_via_liftover(chromosome, bp, build_map):
    if is_number(bp):
        data = build_map.convert_coordinate('chr' + str(chromosome), int(bp)-1)
        if data is not None:
            if len(data) > 0:
                return data[0][0].replace("chr",""), int(data[0][1])+1
    return "Unable to be mapped", None


def map_bp_to_build_via_ensembl(chromosome, bp, from_build, to_build):
    client = EnsemblRestClient()
    data = client.map_bp_to_build_with_rest(
        chromosome, bp, suffixed_release.get(from_build), suffixed_release.get(to_build)
    )
    return data


def open_file_and_process(file, from_build, to_build):
    filename = get_filename(file)
    new_filename = 'liftover_' + filename + '.tsv'
    build_map = None
    if from_build != to_build:
        build_map = LiftOver(ucsc_release.get(from_build), ucsc_release.get(to_build))

    with open(file) as csv_file:
        count = 0
        result_file = open(new_filename, "w")
        csv_reader = csv.DictReader(csv_file, delimiter='\t')
        fieldnames = csv_reader.fieldnames
        writer = csv.DictWriter(result_file, fieldnames=fieldnames, delimiter='\t')

        writer.writeheader()

        for row in csv_reader:
            chromosome = row[CHR_DSET].replace('23', 'X').replace('24', 'Y')
            bp = row[BP_DSET]

            # do the bp location mapping if needed
            if from_build != to_build:
                mapped_bp = map_bp_to_build_via_liftover(chromosome=chromosome, bp=bp, build_map=build_map)
                if mapped_bp is None:
                    mapped_bp = map_bp_to_build_via_ensembl(chromosome=chromosome, bp=bp, from_build=from_build, to_build=to_build)
                row[BP_DSET] = mapped_bp

            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(count)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-original', help='The build of the file', required=True)
    argparser.add_argument('-mapped', help='The build to map to', required=True)
    args = argparser.parse_args()
    file = args.f
    from_build = args.original
    to_build = args.mapped

    open_file_and_process(file, from_build, to_build)

if __name__ == "__main__":
    main()
