import os
from collections import UserDict
from copy import deepcopy
from time import localtime, strftime

import pickle
import dotenv
import pymongo
import numpy as np
import pandas as pd
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
    def __init__(self):
        super().__init__()
        self.data = {
            "material_id":              "",     # str
            "nframes":                  0,      # int
            "elements":                 [],     # list of str
            "nelements":                0,      # int
            "formula":                  "",     # str, metal order，金属-非金属（字母顺序）
            "reduced_formula":          "",     # str, metal order，金属-非金属（字母顺序）
            "natoms":                   0,      # int
            "cell":                     np.zeros([0, 3, 3]),  # np.ndarray, angstrom
            "positions":                np.zeros([0, 0, 3]),  # np.ndarray, angstrom
            "scaled_positions":         np.zeros([0, 0, 3]),  # np.ndarray
            "forces":                   np.zeros([0, 0, 3]),  # np.ndarray
            "volume":                   [],     # np.ndarray, A^3
            "enthalpy":                 [],     # np.ndarray, eV
            "enthalpy_per_atom":        [],     # np.ndarray, eV
            "density":                  [],     # np.ndarray, g/(mol*A^3)->g/cm^3
            "final_enthalpy":           0.0,    # 为更容易查询筛选
            "final_enthalpy_per_atom":  0.0,
            "final_avg_force":          0.0,
            "final_volume":             0.0,
            "final_density":            0.0,
            "calyconfig":               {},
            "incar":                    [""],   # list of str，不要替换换行符
            "potcar":                   [""],   # list of str，不要替换换行符
            "pressure":                 0.0,    # float, GPa
            "clospack_volume":          0.0,    # float, A^3
            "clospack_density":         0.0,    # float, g/(mol*A^3)->g/cm^3
            "last_update":              strftime("%d/%M/%Y %H:%M:%S %z", localtime()),
            "symmetry": {
                "1e-1": {                       # str, symprec, %.0e
                    "number":         0,        # int [1, 230]
                    "symbol":         "",
                    "crystal_system": "",
                },
            },
            "deprecated":               False,  # bool
            "deprecated_reason":        "",     # str
        }  # fmt: skip

    def check(self):
        assert (
            len(self.data["elements"])
            == self.data["nelements"]
            == len(self.data["potcar"])
        )
        assert (
            self.data["natoms"]
            == self.data["positions"].shape[1]
            == self.data["scaled_positions"].shape[1]
            == self.data["forces"].shape[1]
        )
        assert (
            self.data["nframes"]
            == self.data["cell"].shape[0]
            == self.data["positions"].shape[0]
            == self.data["scaled_positions"].shape[0]
            == self.data["forces"].shape[0]
            == len(self.data["volume"])
            == len(self.data["enthalpy"])
            == len(self.data["enthalpy_per_atom"])
            == len(self.data["density"])
        )

    @classmethod
    def fromSeries(cls, series: pd.Series):
        c = cls()
        c.data = {}
        return c

    @classmethod
    def fromAtoms(cls, atoms_list, properties):
        pass
