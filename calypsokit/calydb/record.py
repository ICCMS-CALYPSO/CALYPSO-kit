from collections import UserDict
from datetime import datetime

import numpy as np
from ase import Atoms
from ase.data import atomic_numbers, covalent_radii
from ase.spacegroup import get_spacegroup


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
                "name": "",             # calypso or materials project, etc.
                "index": 0,             # int index in this source, or None
            },
            "elements":                 [],     # list of str
            "nelements":                0,      # int
            "elemcount":                [],     # list of int, count of each element
            "species":                  [],     # list of str, species of each atom
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
                "source_file":              [],     # source path of each frame
                "source_idx":               [],     # index in each source
                "source_dir":               "",     # source dir
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
    def fromAtoms(cls, trajectory, **kwargs):
        formula = trajectory[-1].symbols.formula
        pressure = trajectory[-1].get("pressure", 0.0)
        pressure_range = cls.get_pressure_range(pressure)
        (
            density,
            volume,
            clospack_density,
            clospack_volume,
        ) = cls.get_density_clospack_density(trajectory[-1])
        symmetry = cls.wrapped_get_symmetry(trajectory[:-1])

        datadict = {
            "elements": list(formula.count().keys()),
            "nelements": len(formula.count()),
            "elemcount": list(formula.count().values()),
            "species": trajectory[-1].get_chemical_symbols(),
            "formula": formula.format("metal"),
            "reduced_formula": formula.reduce()[1].format("metal"),
            "natoms": len(trajectory[-1]),
            "cell": trajectory[-1].cell[:],
            "positions": trajectory[-1].positions,
            "scaled_positions": trajectory[-1].get_scaled_positions(),
            "forces": trajectory[-1].info.get(
                "forces", np.zeros([len(trajectory[-1]), 3]) * np.nan
            ),
            "enthalpy": trajectory[-1].info.get("enthalpy", np.nan),
            "enthalpy_per_atom": trajectory[-1].info.get("enthalpy_per_atom", np.nan),
            "volume": volume,
            "volume_per_atom": volume / len(trajectory[-1]),
            "density": density,
            "clospack_volume": clospack_volume,
            "clospack_volume_per_atom": clospack_volume / len(trajectory[-1]),
            "clospack_density": clospack_density,
            "pressure": trajectory[-1].get("pressure", 0.0),
            "pressure_range": pressure_range,
            "trajectory": {
                "nframes": len(trajectory),  # int
                "cell": np.stack([atoms.cell[:] for atoms in trajectory]),
                "positions": np.stack([atoms.positions for atoms in trajectory]),
                "scaled_positions": np.stack(
                    [atoms.get_scaled_positions() for atoms in trajectory]
                ),
                "forces": np.stack(
                    [
                        atoms.info.get("forces", np.zeros([len(atoms), 3]))
                        for atoms in trajectory
                    ]
                ),
                "volume": [atoms.get_volumes() for atoms in trajectory],
                "enthalpy": [
                    atoms.info.get("enthalpy", np.nan) for atoms in trajectory
                ],
                "enthalpy_per_atom": [
                    atoms.info.get("enthalpy_per_atom", np.nan) / len(atoms)
                    for atoms in trajectory
                ],
                "source": [atoms.info.get("source", None) for atoms in trajectory],
                "source_idx": [
                    atoms.info.get("source_idx", None) for atoms in trajectory
                ],
                "source_dir": trajectory[-1].info.get("source_dir", None),
            },
            "calyconfig": trajectory[-1].info.get("calyconfig", None),
            "dftconfig": [atoms.info.get("dftconfig", None) for atoms in trajectory],
            "pseudopotential": trajectory[-1].info.get(
                "pseudopotential", [None] * len(trajectory[-1])
            ),
            "symmetry": symmetry,
            # TODO: recommand to use multiprecessing to deal with core dump
            "donator": {"name": "", "email": ""},  # TODO: add donator
            "deprecated": False,
            "deprecated_reason": "",
        }
        return datadict

    @staticmethod
    def get_density_clospack_density(atoms: Atoms):
        mass = sum(atoms.get_masses())  # g/mol
        volume = atoms.get_volume()  # A^3
        density = mass / volume / 0.602214076  # to g/cm^3
        clospack_volume = sum(
            [
                4 / 3 * np.pi * covalent_radii[atomic_numbers[atom.symbol]] ** 3
                for atom in atoms
            ]
        )
        packingcoef = 0.74048
        clospack_volume /= packingcoef
        clospack_density = (
            mass / clospack_volume
        )  # (g/molA3)  * 6.02 * 1e-27 * 1e30(g/m3)
        return density, volume, clospack_density, clospack_volume

    @staticmethod
    def get_pressure_range(pressure, length=0.2, closed="right") -> dict:
        """put float pressure into string range

        Put the pressure to [-0.1, 0.1, 0.3, ...] bins centered on 0 (with length 0.2
        by default), return the mid of the bin as the representation. There is no worth
        setting length less than 0.1 and it is recommanded to keep 0.2 for compatibility
        with those already stored records.

        Because floating numbers cannot be strictly judged to be equal.
        This range is for group structures which are in a small pressure range.

        Parameters
        ----------
        pressure : float
            pressure
        length : float, optional
            bin length, , by default 0.2
        closed : str, optional
            right for (...] and left for [...), by default "right"

        Returns
        -------
        dict
            {"mid": "x.x", "length": "d.d", "closed": "<closed>"}, save as string format
            1 digit after the decimal point

        Raises
        ------
        NotImplementedError
            Argument closed is not "right" or "left"
        """
        length = abs(length)
        if closed == "right":
            mid = -((-pressure - length / 2) // length + 1) * length
        elif closed == "left":
            mid = ((pressure + length / 2) // length) * length
        else:
            raise NotImplementedError("Argument colsed only support [right|left]")
        if abs(mid) < 1e-3:
            mid = abs(mid)
        return {
            "mid": "{:.1f}".format(mid),
            "length": "{:.1f}".format(length),
            "closed": closed,
        }

    @staticmethod
    def get_symmetry(atoms: Atoms, symprec):
        spg = get_spacegroup(atoms, symprec)
        return {"number": spg.no, "symbol": spg.symbol}

    def warpped_get_symmetry(self, atoms: Atoms):
        return {
            key: self.get_symmetry(atoms, symprec)
            for key, symprec in zip(["1e-1", "1e-2", "1e-5"], [1e-1, 1e-2, 1e-5])
        }
