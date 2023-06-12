import unittest

from ase import Atoms
from pymatgen.core.structure import Structure

from calypsokit.analysis import properties


class TestValidity(unittest.TestCase):
    pass


class TestSimGroup(unittest.TestCase):
    pass


class TestProperties(unittest.TestCase):
    atoms = Atoms(
        "C2",
        positions=[[0, 0, 0], [0.5, 0.5, 0.5]],
        cell=[[2, 0, 0], [0, 2, 0], [0, 0, 2]],
        pbc=True,
    )
    ase_cif = """data_image0
_chemical_formula_structural       C2
_chemical_formula_sum              "C2"
_cell_length_a       2
_cell_length_b       2
_cell_length_c       2
_cell_angle_alpha    90
_cell_angle_beta     90
_cell_angle_gamma    90

_space_group_name_H-M_alt    "P 1"
_space_group_IT_number       1

loop_
  _space_group_symop_operation_xyz
  'x, y, z'

loop_
  _atom_site_type_symbol
  _atom_site_label
  _atom_site_symmetry_multiplicity
  _atom_site_fract_x
  _atom_site_fract_y
  _atom_site_fract_z
  _atom_site_occupancy
  C   C1        1.0  0.00000  0.00000  0.00000  1.0000
  C   C2        1.0  0.25000  0.25000  0.25000  1.0000
"""
    ase_poscar = """ C 
 1.0000000000000000
     2.0000000000000000    0.0000000000000000    0.0000000000000000
     0.0000000000000000    2.0000000000000000    0.0000000000000000
     0.0000000000000000    0.0000000000000000    2.0000000000000000
 C  
   2
Direct
  0.0000000000000000  0.0000000000000000  0.0000000000000000
  0.2500000000000000  0.2500000000000000  0.2500000000000000
"""
    pmg_struct = Structure(
        [[2, 0, 0], [0, 2, 0], [0, 0, 2]],
        ["C", "C"],
        [[0, 0, 0], [0.5, 0.5, 0.5]],
    )

    def test_01_cif_str(self):
        self.assertEqual(properties.get_cif_str(self.atoms), self.ase_cif)

    def test_02_poscar_str(self):
        self.assertEqual(properties.get_poscar_str(self.atoms), self.ase_poscar)
