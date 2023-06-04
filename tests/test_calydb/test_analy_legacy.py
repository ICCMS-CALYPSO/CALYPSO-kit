import unittest

from calypsokit.analysis.legacy.pkldata2doc import (
    get_current_caly_max_index,
    update_timestamp,
)
from calypsokit.calydb.login import login


class TestCalyDBAnalysis(unittest.TestCase):
    # WARNING! Never change this debugcollection
    db, col = login(col="debugcol")

    def setUp(self):
        self.col.delete_many({})

    def tearDown(self):
        self.col.delete_many({})

    def test_01_get_maxcalyidx(self):
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

    def test_02_update_timestamp(self):
        self.col.insert_one({"material_id": "debug-01"})
        self.assertIsNone(self.col.find_one({"last_updated_utc": {"$exists": True}}))
        update_timestamp(self.col)
        self.assertIsNotNone(self.col.find_one({"last_updated_utc": {"$exists": True}}))
