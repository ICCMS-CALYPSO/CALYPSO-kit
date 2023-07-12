import io
from contextlib import redirect_stdout
from typing import Union

import numpy as np
import scipy
from ase import Atoms
from ase.data import atomic_masses, atomic_numbers, covalent_radii
from ase.formula import Formula
from ase.io import write
from ase.spacegroup import get_spacegroup
from pymatgen.analysis.dimensionality import get_dimensionality_larsen
from pymatgen.analysis.local_env import CrystalNN
from pymatgen.core.structure import Structure
from pymatgen.io.cif import CifWriter
from pymatgen.io.vasp import Poscar
from scipy.linalg import polar
from scipy.spatial.transform import Rotation as R


def get_density_clospack_density(
    species: list[str], cell: np.ndarray
) -> tuple[float, float, float, float]:
    """given atom symbol list and cell array, return density and volume and the
    close-pack structure's density and volume

    Parameters
    ----------
    species : list[str]
        list of symbols, for example ["H", "H", "O"]
    cell : np.ndarray
        cell matrix, [[ax, ay, az], [bx, by, bz], [cx, cy, cz]]

    Returns
    -------
    (density, volume, clospack_density, clospack_volume)
        density and volume, unit g/cm^3 and A^3
    """
    mass = float(sum(atomic_masses[atomic_numbers[symbol]] for symbol in species))
    # g/mol
    volume: float = abs(np.linalg.det(cell))  # A^3
    gmol2gcm3 = 1 / 0.602214076
    density = mass / volume * gmol2gcm3  # to g/cm^3
    clospack_volume: float = sum(
        [
            4 / 3 * np.pi * covalent_radii[atomic_numbers[symbol]] ** 3
            for symbol in species
        ]
    )
    packingcoef = 0.74048
    clospack_volume /= packingcoef
    clospack_density = mass / clospack_volume * gmol2gcm3
    return density, volume, clospack_density, clospack_volume


def get_atoms_number_of_structure(structure: Union[Atoms, Structure]):
    """given structure and return the number of atoms

    Parameters
    ----------
    structure : Structure
        Atoms : ase Atoms object
        Structure : pymatgen Structure object

    Returns
    -------
    number_of_atoms : int
    """

    if isinstance(structure, Atoms):
        natoms = len(structure)
    elif isinstance(structure, Structure):
        natoms = structure.composition.num_atoms
    else:
        raise ValueError(f"Unknown type of {structure=}")

    return natoms


def get_formula(species: list[str]) -> tuple[str, str]:
    """Given atom symbol list, return formula and reduced formula in "metal" format

    Parameters
    ----------
    species : list[str]
        atom symbol list

    Returns
    -------
    formula: str, "metal" format
    reduced_formula: str, "metal" format
    """
    formula = Formula("".join(species))
    reduced_formula = formula.reduce()[0]
    return formula.format("metal"), reduced_formula.format("metal")


def get_pressure_range(pressure: float, length=0.2, closed="right") -> dict:
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


def get_crystal_system(n: int) -> str:
    """Get the crystal system for the structure, e.g., (triclinic, orthorhombic,
    cubic, etc.).

        Parameters
        ----------
        n : int
            space group number

        Returns
        -------
        crystal_system: str
            crystal system,
            triclinic|monoclic|orthorhombic|tetragonal|trigonal|hexagonal|cubic

        Raises
        ------
        ValueError
            space group number is not int or not in [1, 230]
    """
    if not isinstance(n, int):
        raise ValueError(f"Invalid space group number: {n}")
    elif n < 0:
        raise ValueError(f"Invalid space group number: {n}")
    elif n < 3:
        return "triclinic"
    elif n < 16:
        return "monoclinic"
    elif n < 75:
        return "orthorhombic"
    elif n < 143:
        return "tetragonal"
    elif n < 168:
        return "trigonal"
    elif n < 195:
        return "hexagonal"
    elif n < 231:
        return "cubic"
    else:
        raise ValueError(f"Invalid space group number: {n}")


def get_symmetry(atoms: Atoms, symprec=0.01):
    """given atoms and symprec, return a dictory containing 
    spacegroup number, spacegroup symbol and crystal system

    Parameters
    ----------
    atoms : Atoms
        Atoms object
    symprec : np.float
        default: 0.1, 

    Returns
    -------
    {spacegroup_number, spacegroup_symbol, crystal_system}
    """
    spg = get_spacegroup(atoms, symprec)
    return {
        "number": spg.no,
        "symbol": spg.symbol,
        "crystal_system": get_crystal_system(spg.no),
    }


def wrapped_get_symmetry(atoms: Atoms):
    """wrap the `get_symmetry` and return symmetry[1e-1, 1e-2, 1e-5]

    Parameters
    ----------
    atoms : Atoms
        Atoms object

    Returns
    -------
    dict[str, dict]
        {"1e-1": {...}, "1e-2": {...}, "1e-5": {...}}, the value is
        same as the return value of `get_symmetry`
    """
    return {
        key: get_symmetry(atoms, symprec)
        for key, symprec in zip(["1e-1", "1e-2", "1e-5"], [1e-1, 1e-2, 1e-5])
    }


def wrapped_get_symmetry_from_datadict(datadict):
    """given a datadict[species, cell, scaled_positions] and 
       return symmetry[1e-1, 1e-2, 1e-5]

    Parameters
    ----------
    datadict : dict
      keys: species, cell, scaled_positions

    Returns
    -------
    dict[str, dict]
        {"1e-1": {...}, "1e-2": {...}, "1e-5": {...}}, the value is
        same as the return value of `get_symmetry`
    """
    species = datadict["species"]
    cell = datadict["cell"]
    scaled_positions = datadict["scaled_positions"]
    atoms = Atoms(species, cell=cell, scaled_positions=scaled_positions)
    return wrapped_get_symmetry(atoms)


def get_min_distance(structure: Union[Atoms, Structure]):
    """calculate min dis of given strucutre and
       consider the image atom distance (min(a, b, c)) and PBC min distance

    Parameters
    ----------
    structure : Union[Atoms, Structure]
       Atoms: ase Atoms object
       Structure: pymatgen Structure object

    Returns
    -------
    min distance : np.float
    """
    if isinstance(structure, Atoms):
        distmat = structure.get_all_distances(mic=True)
        selfmin = min(structure.cell.cellpar()[:3])
    elif isinstance(structure, Structure):
        distmat = structure.distance_matrix
        selfmin = min(structure.lattice.abc)
    else:
        raise ValueError("Unknow type or structure, neither ase nor pymatgen")
    row, col = np.diag_indices_from(distmat)
    distmat[row, col] = selfmin
    return np.min(distmat)


def kabsch(P, Q):
    """Calculate the optimal rotation matrix using the Kabsch algorithm.

    Parameters
    ----------
    P, Q : ndarray
        nx3 matrix of coordinates representing the first and second set of points.

    Returns:
    --------
    R : ndarray
        The optimal 3x3 rotation matrix.
    """

    # Calculate the covariance matrix
    C = np.matmul(np.transpose(P), Q)

    # Calculate the singular value decomposition (SVD) of the covariance matrix
    V, S, Wt = np.linalg.svd(C)

    # Ensure the proper orientation of the rotation matrix
    d = (np.linalg.det(V) * np.linalg.det(Wt)) < 0.0

    if d:
        S[-1] = -S[-1]
        V[:, -1] = -V[:, -1]

    # Calculate the optimal rotation matrix
    kabsch_rot = np.dot(V, Wt)

    return kabsch_rot


def get_kabsch_info(celli, cellr):
    """Calculate two cell matrix distance using 
    or not using the Kabsch algorithm.

    Parameters
    ----------
    celli, cellr : ndarray
        3x3 matrix of ini or relaxed cell.

    Returns:
    --------
    kabsh_info : dict[kabsh_rot,
                      max_d_cell_i_r,
                      avg_d_cell_i_r, 
                      max_d_cell_roti_r,
                      avg_d_cell_roti_r]
    """

    kabsch_rot = kabsch(celli, cellr)
    max_d_cell_i_r = np.max(np.abs(celli - cellr))
    avg_d_cell_i_r = np.mean(np.abs(celli - cellr))
    cellirot = np.matmul(celli, kabsch_rot)
    max_d_cell_roti_r = np.max(np.abs(cellirot - cellr))
    avg_d_cell_roti_r = np.mean(np.abs(cellirot - cellr))

    return {
        "kabsch_rot": kabsch_rot,
        "max_d_cell_i_r": max_d_cell_i_r,
        "avg_d_cell_i_r": avg_d_cell_i_r,
        "max_d_cell_roti_r": max_d_cell_roti_r,
        "avg_d_cell_roti_r": avg_d_cell_roti_r,
    }


def get_shifted_d_frac(fraci, fracr):
    """Calculate distance of two frac coordinate 

    Parameters
    ----------
    fraci, fracr : ndarray
        nx3 matrix of ini or relaxed coordinates.

    Returns:
    --------
    shifted_d_frac : dict[shifted_d_frac,
                          max_shifted_d_frac,
                          avg_shifted_d_frac]
    """

    shifted_d_frac = (fracr - fraci + 0.5) % 1 - 0.5
    max_shifted_d_frac = np.max(np.abs(shifted_d_frac))
    avg_shifted_d_frac = np.mean(np.abs(shifted_d_frac))

    return {
        "shifted_d_frac": shifted_d_frac,
        "max_shifted_d_frac": max_shifted_d_frac,
        "avg_shifted_d_frac": avg_shifted_d_frac,
    }


def polar_decompose(A, B):  # -> U, P
    """Polar decompose, on column-array

    .. math:: B = UPA

    Parameters
    ----------
    A, B : ndarray
        3x3 ndarray

    Returns
    -------
    U, P : ndarray
        3x3 ndarray
    """
    U, P = polar(B @ np.linalg.inv(A))
    return U, P


def get_avg_strain(P):
    eps = P - np.eye(3)
    I1 = eps[0, 0] + eps[1, 1] + eps[2, 2]
    I2 = (
        eps[1, 1] * eps[2, 2]
        + eps[2, 2] * eps[0, 0]
        + eps[0, 0] * eps[1, 1]
        - eps[1, 2] ** 2
        - eps[2, 0] ** 2
        - eps[0, 1] ** 2
    )
    avg_strain = np.sqrt((I1**2 - 2 * I2) / 6)
    return float(avg_strain)


def get_strain_info(celli, cellr):
    U, P = polar_decompose(celli.T, cellr.T)
    strain = P - np.eye(3)
    avg_strain = get_avg_strain(P)
    return {"strain": strain, "avg_strain": avg_strain, "rot": U}


def as_euler(U, seq="ZXZ"):
    rot = R.from_matrix(U)
    return rot.as_euler(seq)


def as_quat(U):
    rot = R.from_matrix(U)
    return rot.as_quat()


crystalnn = CrystalNN()


def get_dim_larsen(structure: Structure):
    """calculate dimension of given structure

    Parameters
    ----------
    structure : Structure

    Returns
    -------
    dim_larsen : np.int
    """

    try:
        bonded_structure = crystalnn.get_bonded_structure(structure)
        dim_larsen = int(get_dimensionality_larsen(bonded_structure))
    except Exception:
        dim_larsen = -1
    return dim_larsen


def get_cif_str(structure):
    """given structure and return the string of cif

    Parameters
    ----------
    structure : Structure

    Returns
    -------
    cif : string
    """

    if isinstance(structure, Atoms):
        with io.BytesIO() as buffer, redirect_stdout(buffer):  # type: ignore [type-var]
            write('-', structure, format='cif')
            cif = buffer.getvalue().decode()
    elif isinstance(structure, Structure):
        cif = str(CifWriter(structure).ciffile)
    else:
        raise ValueError(f"Unknown type of {structure=}")

    return cif


def get_poscar_str(structure: Union[Atoms, Structure]):
    """given structure and return the string of poscar

    Parameters
    ----------
    structure : Structure

    Returns
    -------
    cif : string
    """

    if isinstance(structure, Atoms):
        with io.StringIO() as buffer, redirect_stdout(buffer):
            write('-', structure, format='vasp', direct=True)
            vasp = buffer.getvalue()  # byte to string
    elif isinstance(structure, Structure):
        vasp = Poscar(structure).get_string()
    else:
        raise ValueError(f"Unknown type of {structure=}")
    return vasp


def get_min_dis_of_diff_elems(atoms, type_map):
    '''
    type_map=['Li','La','H']
    return: min dis of different type bonds including
             `Li-La`, `Li-H`, `La-H`, `Li-Li`, `La-La`, `H-H`
    '''
    diff_bond_type = list(map(list, combinations(type_map, 2)))
    diff_bond_type.extend([[same_type,same_type] for same_type in type_map])

    atoms_number = atoms.symbols.formula.count().values()
    if 1 in atoms_number:
        atoms = make_supercell(atoms, [[2, 0, 0], [0, 2, 0], [0, 0, 2]])

    bond_situation = {}
    for each_type in diff_bond_type:
        bond_situation_key = ''.join(each_type)
        ana = Analysis(atoms)
        bond_indice = ana.get_bonds(each_type[0], each_type[1], unique=True)
        if bond_indice == [[]]:
            min_dis = np.array(203)
            #min_dis = np.nan
        else:
            bond_value = ana.get_values(bond_indice)
            min_dis = np.nanmin(bond_value[0])
        bond_situation[bond_situation_key] = min_dis

    return bond_situation

