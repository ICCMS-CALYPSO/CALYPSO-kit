import os
import pickle
import sys
import unittest
from pprint import pprint

import numpy as np
import pandas as pd
import pyarrow as pa
from pymongo.errors import DuplicateKeyError

from calypsokit.calydb.calydb import RawDocDict, bideser, biser, connect_to_db


class TestCalyDB(unittest.TestCase):
    # WARNING! Never change this debugcollection
    db, col = connect_to_db(col="debugcol")

    def setUp(self):
        self.col.delete_many({})

    def tearDown(self):
        self.col.delete_many({})

    def test_01_query(self):
        self.col.insert_one({"material_id": "debug-01"})
        self.assertEqual(len(list(self.col.find({"material_id": "debug-01"}))), 1)

    def test_02_sl_ndarray(self):
        ar = np.random.rand(3, 2)
        doc = {"material_id": "debug-01", "ndarray": biser(ar)}
        self.col.insert_one(doc)
        record = self.col.find_one({"material_id": "debug-01"})
        self.assertIsNotNone(record)
        self.assertTrue(np.allclose(bideser(record["ndarray"]), ar))

    def test_03_duplicate_error(self):
        doc = {"material_id": "debug-01"}
        self.col.insert_one(doc)
        self.assertRaises(DuplicateKeyError, self.col.insert_one, doc)

    def test_04_RawDocDict(self):
        rawdocdict = RawDocDict()
        rawdocdict = RawDocDict(pd.Series({"material_id": "debug-01"}))
        # pprint(rawdocdict)
        pass


def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCalyDB))
    unittest.TextTestRunner(verbosity=2).run(suite)
