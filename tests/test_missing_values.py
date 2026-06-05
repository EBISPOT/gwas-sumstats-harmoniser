import importlib
import math
import os
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace


REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
BIN_DIR = os.path.join(REPO_ROOT, "bin")
sys.path.insert(0, BIN_DIR)

from common_constants import MISSING_VALUE, is_missing_value, normalise_missing_value


def import_main_pysam_with_stubs():
    sys.modules.setdefault("pysam", types.ModuleType("pysam"))

    gwas_module = types.ModuleType("gwas_sumstats_tools")
    interfaces_module = types.ModuleType("gwas_sumstats_tools.interfaces")
    data_table_module = types.ModuleType("gwas_sumstats_tools.interfaces.data_table")

    class SumStatsTable:
        def __init__(self, sumstats_file):
            self.sumstats_file = sumstats_file

        def _set_header_order(self):
            return []

    data_table_module.SumStatsTable = SumStatsTable
    sys.modules.setdefault("gwas_sumstats_tools", gwas_module)
    sys.modules.setdefault("gwas_sumstats_tools.interfaces", interfaces_module)
    sys.modules.setdefault("gwas_sumstats_tools.interfaces.data_table", data_table_module)

    return importlib.import_module("main_pysam")


class MissingValueHelpersTest(unittest.TestCase):
    def test_recognises_standard_and_legacy_missing_tokens(self):
        missing_values = [MISSING_VALUE, "NA", "NaN", "nan", "", " ", "  ", ".", "-", "none", "null", "nil", None, math.nan]

        for value in missing_values:
            with self.subTest(value=value):
                self.assertTrue(is_missing_value(value))

    def test_preserves_non_missing_values(self):
        for value in ["rs123", "0", 0, "A", "not-null"]:
            with self.subTest(value=value):
                self.assertFalse(is_missing_value(value))
                self.assertEqual(normalise_missing_value(value), value)

    def test_normalises_missing_values_to_gwas_ssf_token(self):
        self.assertEqual(normalise_missing_value("NA"), MISSING_VALUE)
        self.assertEqual(normalise_missing_value("."), MISSING_VALUE)
        self.assertEqual(normalise_missing_value(None), MISSING_VALUE)


class MainPysamMissingParsingTest(unittest.TestCase):
    def test_parse_sum_stats_converts_all_missing_tokens_to_none(self):
        main_pysam = import_main_pysam_with_stubs()
        main_pysam.args = SimpleNamespace(na_rep_in="legacy_missing")

        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write("chromosome\trsid\tp_value\tinfo\n")
            handle.write("1\t#NA\t0.1\tkeep\n")
            handle.write("1\tNA\t.\tlegacy\n")
            handle.write("1\tlegacy_missing\t \t0.2\n")
            handle.write("1\tkeep\t0.3\t\n")
            path = handle.name

        try:
            rows = list(main_pysam.parse_sum_stats(path, "\t"))
        finally:
            os.unlink(path)

        self.assertIsNone(rows[0]["rsid"])
        self.assertEqual(rows[0]["p_value"], "0.1")
        self.assertEqual(rows[0]["info"], "keep")
        self.assertIsNone(rows[1]["rsid"])
        self.assertIsNone(rows[1]["p_value"])
        self.assertIsNone(rows[2]["rsid"])
        self.assertIsNone(rows[2]["p_value"])
        self.assertEqual(rows[2]["info"], "0.2")
        self.assertEqual(rows[3]["rsid"], "keep")
        self.assertIsNone(rows[3]["info"])


class BasicQcMissingValueTest(unittest.TestCase):
    def test_blanks_to_na_uses_canonical_token(self):
        ensembl_module = types.ModuleType("ensembl_rest_client")
        ensembl_module.EnsemblRestClient = object
        sys.modules.setdefault("ensembl_rest_client", ensembl_module)
        basic_qc = importlib.import_module("basic_qc_nf")
        row = ["rs123", "", "NA", ".", "nil", "value"]

        self.assertEqual(
            basic_qc.blanks_to_NA(row),
            ["rs123", MISSING_VALUE, MISSING_VALUE, MISSING_VALUE, MISSING_VALUE, "value"],
        )

    def test_required_fields_treat_hash_na_as_missing(self):
        ensembl_module = types.ModuleType("ensembl_rest_client")
        ensembl_module.EnsemblRestClient = object
        sys.modules.setdefault("ensembl_rest_client", ensembl_module)
        basic_qc = importlib.import_module("basic_qc_nf")
        header = ["rsid", "p_value", "chromosome", "base_pair_location"]
        row = [MISSING_VALUE, "0.1", "1", "123"]

        self.assertTrue(basic_qc.remove_row_if_required_is_blank(row, header))


class MapToBuildMissingValueTest(unittest.TestCase):
    def test_added_columns_use_canonical_missing_value(self):
        sys.modules.setdefault("duckdb", types.ModuleType("duckdb"))
        sys.modules.setdefault("pandas", types.ModuleType("pandas"))
        liftover_module = types.ModuleType("liftover")
        liftover_module.ucsc_release = {}
        sys.modules.setdefault("liftover", liftover_module)
        map_to_build = importlib.import_module("map_to_build_nf")

        class FakeFrame(dict):
            @property
            def columns(self):
                return list(self.keys())

        frame = FakeFrame()
        map_to_build.add_column_to_df(frame, "rsid")

        self.assertEqual(frame["rsid"], MISSING_VALUE)


if __name__ == "__main__":
    unittest.main()
