import unittest
from datetime import datetime, timedelta
from pprint import pprint

import numpy as np
import pandas as pd
from pymongo.errors import DuplicateKeyError

from calypsokit.calydb.login import login
from calypsokit.calydb.record import RecordDict


class TestCalyDB(unittest.TestCase):
    # WARNING! Never change this debugcollection
    db, col = login(col="debugcol")

    def setUp(self):
        self.col.delete_many({})

    def tearDown(self):
        self.col.delete_many({})

    def test_01_query(self):
        self.col.insert_one({"material_id": "debug-01"})
        self.assertEqual(len(list(self.col.find({"material_id": "debug-01"}))), 1)

    def test_02_insert_ndarray(self):
        ar = np.random.rand(3, 2)
        doc = {"material_id": "debug-01", "ndarray": {"1": ar}, "abc": None}
        self.col.insert_one(doc)
        record = self.col.find_one({"material_id": "debug-01"})
        self.assertIsNotNone(record)
        self.assertTrue(np.allclose(record["ndarray"]["1"], ar))

    def test_03_duplicate_error(self):
        doc = {"material_id": "debug-01"}
        self.col.insert_one(doc)
        self.assertRaises(DuplicateKeyError, self.col.insert_one, doc)

    def test_04_timestamp(self):
        timestamp = datetime.utcnow()
        doc = {"material_id": "debug-01", "last_updated_utc": timestamp}
        self.col.insert_one(doc)
        record = self.col.find_one({"material_id": "debug-01"})
        self.assertTrue(
            abs(record["last_updated_utc"] - timestamp) < timedelta(seconds=0.001)
        )

    def test_05_RawDocDict(self):
        record = RecordDict()
        self.col.insert_one(record)


def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCalyDB))
    unittest.TextTestRunner(verbosity=2).run(suite)
