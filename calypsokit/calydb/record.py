from collections import UserDict
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
            "source": {
                "name": "calypso",      # calypso or materials project, etc.
                "index": 0,             # int index in this source, or None
            },
            "elements":                 [],     # list of str
            "nelements":                0,      # int
            "formula":                  "",     # str, metal and alphabet order
            "reduced_formula":          "",     # str, metal and alphabet order
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
            "clospack_density":         0.0,    # float, g/cm^3
            "pressure":                 0.0,    # float, GPa
            "pressure_range": {             # each pressure is set to a bin
                "mid":          "0.0",      # starts=-0.1, width=0.2
                "length":       "0.2",      # e.g. 10 -> (-9.9, 10.1]
                "closed":       "right"     # default left-open right-closed
            },                              # for group structures

            "trajectory": {
                "nframes":                  0,      # int
                "cell":                     np.zeros([0, 3, 3]),  # np.ndarray, angstrom
                "positions":                np.zeros([0, 0, 3]),  # np.ndarray, angstrom
                "scaled_positions":         np.zeros([0, 0, 3]),  # np.ndarray
                "forces":                   np.zeros([0, 0, 3]),  # np.ndarray
                "volume":                   [],     # np.ndarray, A^3
                "enthalpy":                 [],     # np.ndarray, eV
                "enthalpy_per_atom":        [],     # np.ndarray, eV
                "source":                   [],     # source path of each frame
                "source_idx":               [],     # index in each source
            },

            "calyconfig": {
                "version": "",
                "icode": 0,
            },
            "dftconfig":                [],   # list of str，不要替换换行符
            "pseudopotential":          [],   # list of str，与elements对应
            "symmetry": {
                # symprec(str %.0e)
                #        int[1, 230] str ("F m -3 m")
                "1e-1": {"number": 0, "symbol": ""},
                "1e-2": {"number": 0, "symbol": ""},
                "1e-5": {"number": 0, "symbol": ""},
            },
            "donator": {"name": "", "email": ""},
            "deprecated":               False,  # bool
            "deprecated_reason":        "",     # str
            "last_updated_utc": datetime.utcnow(),
        }  # fmt: skip
        default_data.update(self.data)
        self.data = default_data
        self.check()

    def check(self):
        assert (
            len(self.data["elements"])
            == self.data["nelements"]
            == len(self.data["pseudopotential"])
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
        ), "nframes not match"

    @classmethod
    def fromAtoms(cls, atoms_list, properties):
        pass
