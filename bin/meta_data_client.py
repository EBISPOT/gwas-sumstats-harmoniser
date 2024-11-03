import yaml
import argparse

class SumStatsMetaData:
    def __init__(self):
        self.metadata = {}

    def read_from_file(self, yaml_in):
        with open(yaml_in, 'r') as in_file:
            self.metadata = yaml.safe_load(in_file)
        return self.metadata

    def write_to_file(self, yaml_out):
        with open(yaml_out, 'w') as out_file:
            yaml.dump(self.metadata, out_file)

    def set_field_to_value(self, field, value):
        self.metadata[field] = value


def parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-i', help='The metadata in file', required=False)
    argparser.add_argument('-o', help='The metadata out file', required=False)
    args = argparser.parse_args()

    in_file = args.i
    out_file = args.o
    return in_file, out_file


def main():
    in_file, out_file = parse_args()
    meta = SumStatsMetaData()
    meta.read_from_file(in_file)
    # set fields
    meta.write_to_file(out_file)


if __name__ == "__main__":
    main()