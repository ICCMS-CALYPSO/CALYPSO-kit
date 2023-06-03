import warnings
from collections import UserDict
from time import localtime, strftime
from datetime import datetime

import numpy as np


class BaseRecordDict(UserDict):
    def update_time(self):
        self.data["last_updated_utc"] = datetime.utcnow()

    def todb(self):
        return self.data


class RecordDict(BaseRecordDict):
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
            },

            "calyconfig": {
                "version": "",
                "icode": 0,
            },
            "abinitconfig":             [],   # list of str，不要替换换行符
            "pseudopotential":          [],   # list of str，与elements对应
            "symmetry": {
                "1e-1": {                       # str, symprec, %.0e
                    "number":         0,        # int [1, 230]
                    "symbol":         "",
                    "crystal_system": "",
                },
                "1e-2": {                       # str, symprec, %.0e
                    "number":         0,        # int [1, 230]
                    "symbol":         "",
                    "crystal_system": "",
                },
            },

            "donator": {
                "name": "",
                "email": "",
            },
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
