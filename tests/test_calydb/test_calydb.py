import os
import sys
import unittest

import calypsokit.calydb.calydb as calydb
import numpy as np


class TestCalyDB(unittest.TestCase):
    # WARNING! Never change this debugcollection
    db, col = calydb.connect_to_db(col="debugcollection")

    def setUp(self):
        self.col.delete_many({})

    def tearDown(self):
        self.col.delete_many({})

    def test01_query(self):
        self.col.insert_one({"material_id": "debug-01"})
        self.assertEqual(len(list(self.col.find({"material_id": "debug-01"}))), 1)


def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCalyDB))
    unittest.TextTestRunner(verbosity=2).run(suite)
