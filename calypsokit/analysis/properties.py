import numpy as np
from ase import Atoms
from ase.data import atomic_masses, atomic_numbers, covalent_radii
from ase.formula import Formula
from ase.spacegroup import get_spacegroup


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
    mass: float = sum(atomic_masses[atomic_numbers[symbol]] for symbol in species)
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


def get_symmetry(atoms: Atoms, symprec):
    spg = get_spacegroup(atoms, symprec)
    return {"number": spg.no, "symbol": spg.symbol}


def wrapped_get_symmetry(atoms: Atoms):
    return {
        key: get_symmetry(atoms, symprec)
        for key, symprec in zip(["1e-1", "1e-2", "1e-5"], [1e-1, 1e-2, 1e-5])
    }


def wrapped_get_symmetry_from_datadict(datadict):
    species = datadict["species"]
    cell = datadict["cell"]
    positions = datadict["positions"]
    atoms = Atoms(species, cell=cell, positions=positions)
    return wrapped_get_symmetry(atoms)
