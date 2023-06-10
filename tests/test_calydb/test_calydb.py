import unittest
from datetime import datetime, timedelta
from pprint import pprint

import numpy as np
import pandas as pd
from ase import Atoms
from pymongo.errors import DuplicateKeyError
from pymatgen.core.structure import Structure

from calypsokit.calydb.login import login, maintain_indexes
from calypsokit.calydb.record import RecordDict
from calypsokit.calydb.queries import QueryStructure
from calypsokit.calydb.readout import ReadOut


class TestCalyDB(unittest.TestCase):
    # WARNING! Never change this debugcollection
    db = login()
    debugcol = db.get_collection("debugcol")
    # recordsample = RecordDict(
    # )

    def setUp(self):
        self.assertEqual(
            self.debugcol.name, "debugcol", "CAN ONLY operate on 'debugcol'"
        )
        self.debugcol.delete_many({})

    def tearDown(self):
        self.debugcol.delete_many({})

    def test_01_basic_query(self):
        self.debugcol.insert_one({"material_id": "debug-01"})
        self.assertEqual(len(list(self.debugcol.find({"material_id": "debug-01"}))), 1)

    def test_02_insert_ndarray(self):
        ar = np.random.rand(3, 2)
        doc = {"material_id": "debug-01", "ndarray": {"1": ar}, "abc": None}
        self.debugcol.insert_one(doc)
        record = self.debugcol.find_one({"material_id": "debug-01"})
        self.assertIsNotNone(record)
        self.assertTrue(np.allclose(record["ndarray"]["1"], ar))

    def test_03_duplicate_error(self):
        doc = {"material_id": "debug-01"}
        self.debugcol.insert_one(doc)
        self.assertRaises(DuplicateKeyError, self.debugcol.insert_one, doc)

    def test_04_timestamp(self):
        timestamp = datetime.utcnow()
        doc = {"material_id": "debug-01", "last_updated_utc": timestamp}
        self.debugcol.insert_one(doc)
        record = self.debugcol.find_one({"material_id": "debug-01"})
        self.assertTrue(
            abs(record["last_updated_utc"] - timestamp) < timedelta(seconds=0.001)
        )

    def test_05_RawDocDict(self):
        record = RecordDict()
        self.debugcol.insert_one(record)
        self.assertEqual(record.get_pressure_range(-0.1, 0.2, "right")["mid"], "-0.2")
        self.assertEqual(record.get_pressure_range(0.1, 0.2, "right")["mid"], "0.0")
        self.assertEqual(record.get_pressure_range(0.2, 0.2, "right")["mid"], "0.2")
        self.assertEqual(record.get_pressure_range(0.4, 0.2, "right")["mid"], "0.4")
        self.assertEqual(record.get_pressure_range(-0.1, 0.2, "left")["mid"], "0.0")
        self.assertEqual(record.get_pressure_range(0.1, 0.2, "left")["mid"], "0.2")
        self.assertEqual(record.get_pressure_range(0.2, 0.2, "left")["mid"], "0.2")
        self.assertEqual(record.get_pressure_range(0.4, 0.2, "left")["mid"], "0.4")
        # self.assertEqual(record.get_pressure_range(0.1, 0.2, "right")["mid"], "0.0")

    def test_06_update(self):
        err_key = "err_key"
        doc = {"key1": "val_1", "key2": {"key2_1": "val2_1", err_key: "val2_2"}}
        # insert an error key-val
        self.debugcol.insert_one(doc)
        record = self.debugcol.find_one(
            {f"key2.{err_key}": {"$exists": 1}}, {"key2": {err_key: 1}}
        )
        val = record["key2"][err_key]
        # fix the error key-val
        self.debugcol.update_one(
            {},
            {"$set": {"key2.key2_2": val}, "$unset": {f"key2.{err_key}": 1}},
        )
        self.assertEqual(self.debugcol.find_one()["key2"]["key2_2"], val)

    def test_07_maintainIndex(self):
        index = maintain_indexes(self.debugcol)
        self.assertTrue(index.get("material_id_1", False))
        self.assertTrue(index.get("deprecated_1", False))

    def test_08_QueryStructure(self):
        rawcol = self.db.get_collection("rawcol")
        qs = QueryStructure(rawcol, trajectory=False, type="pmg")
        self.assertTrue(isinstance(qs.find_one({})[0], Structure))
        qs = QueryStructure(rawcol, trajectory=False, type="ase")
        self.assertTrue(isinstance(qs.find_one({})[0], Atoms))
        qs = QueryStructure(rawcol, trajectory=True, type="pmg")
        self.assertTrue(isinstance(qs.find_one({})[0][0], Structure))
        qs = QueryStructure(rawcol, trajectory=True, type="ase")
        self.assertTrue(isinstance(qs.find_one({})[0][0], Atoms))

    def test_09_readout_cdvae(self):
        rawcol = self.db.get_collection("rawcol")
        uniqcol = self.db.get_collection("uniqcol")
        readout = ReadOut()
        df = readout.unique2cdvae(rawcol, uniqcol, debug=10)
        self.assertEqual(len(df), 10)
