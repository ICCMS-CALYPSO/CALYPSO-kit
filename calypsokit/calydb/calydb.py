import os
import pickle
import warnings
from collections import UserDict
from copy import deepcopy
from time import localtime, strftime

import dotenv
import numpy as np
import pandas as pd
import pymongo
from bson import Binary, ObjectId


def connect_to_db(addr=None, user=None, pwd=None, db=None, col=None, dotenv_path=None):
    dotenv.load_dotenv(dotenv_path=dotenv_path, override=True)
    addr = os.environ.get('MONGO_ADDR', None) if addr is None else addr
    user = os.environ.get('MONGO_USER', None) if user is None else user
    pwd = os.environ.get('MONGO_PWD', None) if pwd is None else pwd
    db = os.environ.get('MONGO_DB', None) if db is None else db
    col = os.environ.get('MONGO_COLLECTION', None) if col is None else col
    for key in (addr, user, pwd, db, col):
        if key is None:
            raise ValueError(f"{key} not configured in .env file")

    client = pymongo.MongoClient(f"mongodb://{user}:{pwd}@{addr}")
    db = client[db]
    col = db[col]

    all_index = col.index_information()
    if all_index.get("material_id", False):
        col.ensure_index([("material_id", 1)], unique=True)
    else:
        col.create_index([("material_id", 1)], unique=True)

    return db, col


def biser(ar):
    """binary serialization"""
    bar = Binary(pickle.dumps(ar, protocol=2), subtype=128)
    return bar


def bideser(bar):
    """binary deserialization"""
    ar = pickle.loads(bar)
    return ar


class DocDict(UserDict):
    def update_time(self):
        self.data["last_update"] = strftime("%d/%M/%Y %H:%M:%S %z", localtime())

    def todb(self):
        data = deepcopy(self.data)
        for key, value in data.items():
            if isinstance(value, np.ndarray):
                data[key] = biser(value)
        return data


class RawDocDict(DocDict):
    def __init__(self, initdata=None):
        super().__init__(initdata)
        default_data = {
            "material_id":              "",     # str
            "elements":                 [],     # list of str
            "nelements":                0,      # int
            "formula":                  "",     # str, metal order，金属-非金属（字母顺序）
            "reduced_formula":          "",     # str, metal order，金属-非金属（字母顺序）
            "natoms":                   0,      # int
            "cell":                     np.zeros([3, 3]),  # np.ndarray, angstrom
            "positions":                np.zeros([0, 3]),  # np.ndarray, angstrom
            "scaled_positions":         np.zeros([0, 3]),  # np.ndarray
            "forces":                   np.zeros([0, 3]),  # np.ndarray
            "enthalpy":           0.0,
            "enthalpy_per_atom":  0.0,
            "volume":             0.0,
            "volume_per_atom":    0.0,
            "density":            0.0,
            "clospack_volume":          0.0,    # float, A^3
            "clospack_volume_per_atom": 0.0,    # float, A^3
            "clospack_density":         0.0,    # float, g/(mol*A^3)->g/cm^3
            "pressure":                 0.0,    # float, GPa

            "trajectory": {
                "nframes":                  0,      # int
                "cell":                     np.zeros([0, 3, 3]),  # np.ndarray, angstrom
                "positions":                np.zeros([0, 0, 3]),  # np.ndarray, angstrom
                "scaled_positions":         np.zeros([0, 0, 3]),  # np.ndarray
                "forces":                   np.zeros([0, 0, 3]),  # np.ndarray
                "volume":                   [],     # np.ndarray, A^3
                "enthalpy":                 [],     # np.ndarray, eV
                "enthalpy_per_atom":        [],     # np.ndarray, eV
                "density":                  [],     # np.ndarray, g/(mol*A^3)->g/cm^3
            },

            "calyconfig": {
                "version": "",
                "ICODE": 0,
            },
            "incar":                    [],   # list of str，不要替换换行符
            "potcar":                   [],   # list of str，不要替换换行符
            "symmetry": {
                "1e-1": {                       # str, symprec, %.0e
                    "number":         0,        # int [1, 230]
                    "symbol":         "",
                    "crystal_system": "",
                },
            },

            "donator": {
                "name": "",
                "email": "",
            },
            "mtime":              strftime("%d/%M/%Y %H:%M:%S %z", localtime()),
            "timefmt":            "%d/%M/%Y %H:%M:%S %z",
            "deprecated":               False,  # bool
            "deprecated_reason":        "",     # str
        }  # fmt: skip
        default_data.update(self.data)
        self.data = default_data
        try:
            self.check()
        except AssertionError as e:
            warnings.warn(str(e))

    def check(self):
        assert (
            len(self.data["elements"])
            == self.data["nelements"]
            == len(self.data["potcar"])
        ), "nelements not match"
        assert (
            self.data["natoms"]
            == self.data["positions"].shape[0]
            == self.data["scaled_positions"].shape[0]
            == self.data["forces"].shape[0]
            == self.data["trajectory"]["positions"].shape[1]
            == self.data["trajectory"]["scaled_positions"].shape[1]
            == self.data["trajectory"]["forces"].shape[1]
        ), "natoms not match"
        assert (
            self.data["trajectory"]["nframes"]
            == self.data["trajectory"]["cell"].shape[0]
            == self.data["trajectory"]["positions"].shape[0]
            == self.data["trajectory"]["scaled_positions"].shape[0]
            == self.data["trajectory"]["forces"].shape[0]
            == len(self.data["trajectory"]["volume"])
            == len(self.data["trajectory"]["enthalpy"])
            == len(self.data["trajectory"]["enthalpy_per_atom"])
            == len(self.data["trajectory"]["density"])
        ), "nframes not match"

    @classmethod
    def fromAtoms(cls, atoms_list, properties):
        pass
