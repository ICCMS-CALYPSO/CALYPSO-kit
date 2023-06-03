import unittest
from datetime import datetime, timedelta
from pprint import pprint

import numpy as np
import pandas as pd
from pymongo.errors import DuplicateKeyError

from calypsokit.analysis.legacy.pkldata2doc import get_current_caly_max_index
from calypsokit.calydb.login import login
from calypsokit.calydb.record import RecordDict


class TestCalyDBAnalysis(unittest.TestCase):
    # WARNING! Never change this debugcollection
    db, col = login(col="debugcol")

    def setUp(self):
        self.col.delete_many({})

    def tearDown(self):
        self.col.delete_many({})

    def test_01_get_maxidx(self):
        maxid = get_current_caly_max_index(self.col)
        self.assertEqual(maxid, 0)
        self.col.insert_one(
            {"material_id": "debug-01", "source": {"name": "calypso", "index": 1}}
        )
        self.col.insert_one(
            {"material_id": "debug-02", "source": {"name": "calypso", "index": 2}}
        )
        maxid = get_current_caly_max_index(self.col)
        self.assertEqual(maxid, 2)
