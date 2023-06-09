# From given pandas DataFrame to each pickle file
#
# DataFrame keys are:
# ------------------------------------------------------------------------
# ['sid_x', 'comment_ini', 'scaling_ini', 'lattice_ini', 'atom_names_ini',
#  'atom_counts_ini', 'coord_ini', 'sourcepath_ini', 'donator_x',
#  'pressure_x', 'incar_x', 'potcar_x', 'inputdat_x', 'version',
#  'compare_sid', 'sid_y', 'enthalpy_per_atom', 'comment_opt',
#  'scaling_opt', 'lattice_opt', 'atom_names_opt', 'atom_counts_opt',
#  'coord_opt', 'sourcepath_opt', 'donator_y', 'pressure_y', 'incar_y',
#  'potcar_y', 'inputdat_y', 'version_opt']
# ------------------------------------------------------------------------
# pickled data dict are:
# (see followed code, may change in the future)
#


import pickle
import sys
from itertools import chain
from pathlib import Path

import numpy as np
import pandas as pd
from ase import Atoms
from ase.data import atomic_masses, atomic_numbers, covalent_radii
from ase.spacegroup import get_spacegroup
from joblib import Parallel, delayed
from tqdm import tqdm

from calypsokit.analysis.legacy.read_inputdat import readinput
from calypsokit.calydb.login import login

try:
    from calypsokit.analysis.legacy.contactbook import contactbook
except ModuleNotFoundError as e:
    raise e


def get_density_clospack_density(species, cell):
    mass = sum(atomic_masses[atomic_numbers[symbol]] for symbol in species)  # g/mol
    volume = abs(np.linalg.det(cell))  # A^3
    gmol2gcm3 = 1 / 0.602214076
    density = mass / volume * gmol2gcm3  # to g/cm^3
    clospack_volume = sum(
        [
            4 / 3 * np.pi * covalent_radii[atomic_numbers[symbol]] ** 3
            for symbol in species
        ]
    )
    packingcoef = 0.74048
    clospack_volume /= packingcoef
    clospack_density = mass / clospack_volume * gmol2gcm3
    return density, volume, clospack_density, clospack_volume


def get_results_dir(root, level=1):
    results_list = []

    def inner_get_dir(path, n: int):
        if n <= 0:
            return
        for deepfile in Path(path).glob("*"):
            if deepfile.is_dir():
                if deepfile.name.startswith("results"):
                    results_list.append(str(deepfile))
                n -= 1
                inner_get_dir(deepfile, n)
                n += 1
            else:
                pass

    inner_get_dir(root, level)
    return results_list


def get_basic_info(root, results_dir, cwd):
    root_date = Path(root).name
    results_dir = Path(results_dir)
    donator = Path(results_dir).relative_to(root).parts[0]
    donator = contactbook[root_date][donator]

    logfile = results_dir.joinpath("CALYPSO.log")
    print(logfile)
    if logfile.exists():
        with open(logfile, "r") as log:
            for line in log:
                if line.startswith("Program"):
                    version = line.split()[2]
                    break
            else:
                version = "legacy"
    else:
        version = "legacy"

    inputdat = readinput(results_dir.parent.joinpath("input.dat"))
    # inputdat = readinput(input=input_path+'/input.dat')

    incar_list = list(results_dir.parent.glob("INCAR*"))
    potcar = []
    if len(incar_list) == 0:
        # os.system(f'echo "{results_dir}" >> {cwd}/noINCAR-record')
        print(f"{results_dir} No INCAR record")
        pressure = np.nan
        incar = "NotFound"
    else:
        with open(incar_list[-1], "r") as f:
            for line in f:
                if line.strip().startswith("PSTRESS"):
                    pressure = float(line.split("=")[-1]) / 10  # kbar to GPa
        with open(incar_list[-1], "r") as f:
            incar = f.read()
    if results_dir.parent.joinpath("OUTCAR").exists():
        with open(results_dir.parent.joinpath("POTCAR"), "r") as f:
            for line in f:
                if "TITEL" in line:
                    potcar.append(line.strip())
    print(donator)
    print(pressure)
    print(incar)
    print(potcar)
    print(inputdat)
    print(version)

    return (donator, pressure, incar, potcar, inputdat, version)


def extract_ini_structures(root, results_dir, basic_info):
    root = Path(root)  # **/<date>
    results_dir = Path(results_dir)  # **/results
    source_dir = str(results_dir.relative_to(root.parent))  # <date>/**/results
    donator, pressure, incar, potcar, inputdat, version = basic_info

    atom_names = inputdat["nameofatoms"]

    struct = {}
    for ini_fname in results_dir.rglob("pso_ini_*"):
        source = str(ini_fname.relative_to(root.parent))  # <date>/**/pso_ini_*
        # idx_pref: (results/)   "**/pso_ini_*"   (may VSC)
        idx_pref = str(ini_fname.relative_to(results_dir)).replace("pso_ini", "pso_sid")
        with open(ini_fname, 'r') as f:
            lines = f.readlines()

        num_lines = len(lines)
        i = 0
        sid = 0

        while i < num_lines:
            try:
                # Check if the line is the start of one structure
                if (
                    ''.join(lines[i].strip().split('_')).isalpha()
                    or ''.join(lines[i].strip().split('_')).isalnum()
                ) and "Direct" not in lines[i]:
                    i += 1  # Skip the structure identifier line
                    sid += 1

                    # Read the scaling factor and lattice vectors
                    # scaling_factor = float(lines[i])
                    lattice_vectors = [
                        list(map(float, lines[j].split())) for j in range(i + 1, i + 4)
                    ]
                    i += 3

                    # Return dummpy structure if atoms numb line is illegal
                    if len(lines[i + 1].split()) >= 1 and int(lines[i + 1].split()[0]):
                        i += 1
                    else:
                        continue

                    # Read the atom types and their counts
                    # atom_types = lines[i].split()
                    atom_counts = list(map(int, lines[i].split()))

                    # Return dummpy structure if direct line is illegal
                    if "Direct" in lines[i + 1]:
                        i += 1
                    else:
                        continue

                    # Return dummpy structure if coord line is illegal
                    if len(lines[i + 1].split()) == 3:
                        i += 1
                    else:
                        continue

                    # Read the atomic coordinates
                    coordinates = []
                    for j in range(i, i + sum(atom_counts)):
                        coords = list(map(float, lines[j].split()[:3]))
                        coordinates.append(coords)

                    species = list(
                        chain.from_iterable(
                            [
                                [atom_name] * atom_count
                                for atom_name, atom_count in zip(
                                    atom_names, atom_counts
                                )
                            ]
                        )
                    )
                    cell = np.asarray(lattice_vectors)
                    scaled_positions = np.asarray(coordinates)
                    positions = scaled_positions @ cell.T
                    (
                        density,
                        volume,
                        clospack_density,
                        clospack_volume,
                    ) = get_density_clospack_density(species, cell)
                    if volume < 1e-5:
                        raise ValueError("Volume too small")
                    if clospack_volume < 1e-5:
                        raise ValueError("Closepack Volume too small")
                    natoms = sum(atom_counts)
                    _id = f"{idx_pref}#{sid}"
                    struct[_id] = {
                        "elements": atom_names,
                        "elemcount": atom_counts,
                        "nelements": len(atom_names),
                        "species": species,
                        "natoms": natoms,
                        "cell": cell,
                        "positions": positions,
                        "scaled_positions": scaled_positions,
                        "forces": np.zeros_like(coordinates) * np.nan,
                        "volume": volume,
                        "volume_per_atom": volume / natoms,
                        "density": density,
                        "clospack_density": clospack_density,
                        "clospack_volume": clospack_volume,
                        "enthalpy": np.nan,
                        "enthalpy_per_atom": np.nan,
                        "source_dir": source_dir,
                        "source_file": source,
                        "source_idx": sid,
                    }

                i += 1

            except Exception as e:
                print(f"Error extracting structure at line {i + 1}: {str(e)}")

    return struct


def extract_opt_structures(root, results_dir, basic_info):
    root = Path(root)  # **/<date>
    results_dir = Path(results_dir)  # **/results
    source_dir = str(results_dir.relative_to(root.parent))  # <date>/**/results
    donator, pressure, incar, potcar, inputdat, version = basic_info

    atom_names = inputdat["nameofatoms"]

    struct = {}
    for opt_fname in results_dir.rglob("pso_opt_*"):
        source = str(opt_fname.relative_to(root.parent))  # <date>/**/pso_ini_*
        # idx_pref: (results/)   "**/pso_ini_*"   (may VSC)
        idx_pref = str(opt_fname.relative_to(results_dir)).replace("pso_opt", "pso_sid")
        with open(opt_fname, 'r') as f:
            lines = f.readlines()

        num_lines = len(lines)
        i = 0
        sid = 0

        while i < num_lines:
            try:
                # Check if the line is the start of one structure
                if (
                    (
                        ''.join(lines[i].strip().split('_')).isalpha()
                        or ''.join(lines[i].strip().split('_')).isalnum()
                    )
                    and "Direct" not in lines[i]
                    and '610612509' not in lines[i]
                ):
                    enthalpy_per_atom = float(lines[i - 1].strip())
                    i += 1  # Skip the structure identifier line
                    sid += 1

                    # Read the scaling factor and lattice vectors
                    # scaling_factor = float(lines[i])
                    lattice_vectors = [
                        list(map(float, lines[j].split())) for j in range(i + 1, i + 4)
                    ]
                    i += 3

                    # Return dummpy structure if atoms numb line is illegal
                    if len(lines[i + 1].split()) >= 1 and int(lines[i + 1].split()[0]):
                        i += 1
                    else:
                        continue

                    # Read the atom types and their counts
                    # atom_types = lines[i].split()
                    atom_counts = list(map(int, lines[i].split()))

                    # Return dummpy structure if direct line is illegal
                    if "Direct" in lines[i + 1]:
                        i += 1
                    else:
                        continue

                    # Return dummpy structure if coord line is illegal
                    if len(lines[i + 1].split()) == 3:
                        i += 1
                    else:
                        continue

                    # Read the atomic coordinates
                    coordinates = []
                    for j in range(i, i + sum(atom_counts)):
                        coords = list(map(float, lines[j].split()[:3]))
                        coordinates.append(coords)

                    species = list(
                        chain.from_iterable(
                            [
                                [atom_name] * atom_count
                                for atom_name, atom_count in zip(
                                    atom_names, atom_counts
                                )
                            ]
                        )
                    )
                    cell = np.asarray(lattice_vectors)
                    scaled_positions = np.asarray(coordinates)
                    positions = scaled_positions @ cell.T
                    (
                        density,
                        volume,
                        clospack_density,
                        clospack_volume,
                    ) = get_density_clospack_density(species, cell)
                    if volume < 1e-5:
                        raise ValueError("Volume too small")
                    if clospack_volume < 1e-5:
                        raise ValueError("Closepack Volume too small")
                    natoms = sum(atom_counts)
                    _id = f"{idx_pref}#{sid}"
                    struct[_id] = {
                        "elements": atom_names,
                        "elemcount": atom_counts,
                        "nelements": len(atom_names),
                        "species": species,
                        "natoms": natoms,
                        "cell": cell,
                        "positions": positions,
                        "scaled_positions": scaled_positions,
                        "forces": np.zeros_like(coordinates) * np.nan,
                        "volume": volume,
                        "volume_per_atom": volume / natoms,
                        "density": density,
                        "clospack_density": clospack_density,
                        "clospack_volume": clospack_volume,
                        "enthalpy": enthalpy_per_atom * natoms,
                        "enthalpy_per_atom": enthalpy_per_atom,
                        "source_dir": source_dir,
                        "source_file": source,
                        "source_idx": sid,
                    }

                i += 1

            except Exception as e:
                # sys.exit()
                i += 1
                print(f"Error extracting structure at line {i + 1}: {str(e)}")
                continue
    return struct


def match_iniopt(ini_dict, opt_dict):
    matched_keys = list(set(ini_dict.keys()) & set(opt_dict.keys()))
    for keys in matched_keys:
        pass
    return matched_keys


def split_path(series):
    # parent = "/home/wangzy/workplace/xiaoshan/database/calypso/Dalao.Done.bk"
    sid_ini = series.sid_x
    sid_opt = series.sid_y
    ini_path, ini_idx = sid_ini.split("#")
    opt_path, opt_idx = sid_opt.split("#")
    ini_path = ini_path[62:].replace("/database/..", "")
    opt_path = opt_path[62:].replace("/database/..", "")
    return ini_path, opt_path, int(ini_idx), int(opt_idx)


def parse_version(series):
    version = series.version
    if version[0] == "P":
        version = version[18:23]
    else:
        version = "legacy"
    return version


"""
def series2data(series):
    formula = "".join(
        f"{e}{n}" for e, n in zip(series.atom_names_ini, series.atom_counts_ini)
    )
    if len(list(series.coord_ini)) != len(list(series.coord_opt)):
        raise ValueError("number of atoms not match")
    atoms_ini = Atoms(
        formula,
        cell=list(series.lattice_ini),
        scaled_positions=list(series.coord_ini),
        pbc=True,
    )
    atoms_opt = Atoms(
        formula,
        cell=list(series.lattice_opt),
        scaled_positions=list(series.coord_opt),
        pbc=True,
    )
    cell_ini = list(series.lattice_ini)
    cell_opt = list(series.lattice_opt)
    try:
        t_ini = np.linalg.det(cell_ini)
        t_opt = np.linalg.det(cell_opt)
    except Exception as e:
        raise Exception(e)
    else:
        assert np.isfinite(t_ini), "ini cell det infinite"
        assert np.isfinite(t_opt), "opt cell det infinite"
        assert t_ini > 1e-5, "ini volume too small"
        assert t_opt > 1e-5, "opt volume too small"
    formula = atoms_ini.get_chemical_formula("metal")
    reduce_formula = atoms_ini.symbols.formula.reduce()[0].format("metal")
    (
        opt_density,
        volume,
        clospack_volume,
        clospack_density,
    ) = get_density_clospack_density(atoms_opt)
    calyconfig = {"version": parse_version(series)}
    try:
        calyconfig.update(readinput(input_str=series.inputdat_x))
    except Exception:
        raise Exception("input fail")
    ini_path, opt_path, ini_idx, opt_idx = split_path(series)
    try:
        spg1 = get_spacegroup(atoms_opt, 1e-1)
        spg1 = {"number": spg1.no, "symbol": spg1.symbol}
        spg2 = get_spacegroup(atoms_opt, 1e-2)
        spg2 = {"number": spg2.no, "symbol": spg2.symbol}
        spg5 = get_spacegroup(atoms_opt, 1e-5)
        spg5 = {"number": spg5.no, "symbol": spg5.symbol}
    except Exception as e:
        raise e

    d = {
        "elements": list(series.atom_names_ini),
        "nelements": len(series.atom_names_ini),
        "elemcount": series.atom_counts_ini,
        "species": list(
            chain.from_iterable(
                [elem] * count
                for elem, count in zip(series.atom_names_ini, series.atom_counts.ini)
            )
        ),
        "formula": formula,
        "reduced_formula": reduce_formula,
        "natoms": len(atoms_ini),
        "cell": atoms_opt.cell[:],
        "positions": atoms_opt.positions,
        "scaled_positions": atoms_opt.get_scaled_positions(),
        "forces": np.zeros([len(atoms_ini), 3]) * np.nan,
        "enthalpy": series.enthalpy_per_atom * len(atoms_ini),
        "enthalpy_per_atom": series.enthalpy_per_atom,
        "volume": volume,
        "volume_per_atom": volume / len(atoms_ini),
        "density": opt_density,
        "clospack_volume": clospack_volume,
        "clospack_volume_per_atom": clospack_volume / len(atoms_ini),
        "clospack_density": clospack_density,
        "pressure": series.pressure_x,
        "pressure_range": {
            "mid": "{:.1f}".format(series.pressure_interval.mid),
            "length": "{:.1f}".format(series.pressure_interval.length),
            "closed": series.pressure_interval.closed,  # left, *right, both, neither
        },
        "trajectory": {
            "nframes": 2,
            "cell": np.stack([atoms_ini.cell[:], atoms_opt.cell[:]]),
            "positions": np.stack([atoms_ini.positions, atoms_opt.positions]),
            "scaled_positions": np.stack(
                [atoms_ini.get_scaled_positions(), atoms_opt.get_scaled_positions()]
            ),
            "forces": np.zeros([2, len(atoms_ini), 3]) * np.nan,
            "volume": [atoms_ini.get_volume(), atoms_opt.get_volume()],
            "enthalpy": [np.nan, series.enthalpy_per_atom * len(atoms_ini)],
            "enthalpy_per_atom": [np.nan, series.enthalpy_per_atom],
            "source": [ini_path, opt_path],
            "source_idx": [ini_idx, opt_idx],
            "source_dir": str(Path(ini_path).parent),
        },
        "calyconfig": calyconfig,
        "dftconfig": [series.incar_x],
        "pseudopotential": series.potcar_x.split("\n"),
        "symmetry": {
            "1e-1": spg1,
            "1e-2": spg2,
            "1e-5": spg5,
        },
        "donator": {"name": donator_dict[series.donator_x], "email": None},
        "deprecated": False,
        "deprecated_reason": "",
    }
    return d
"""


"""
df = pd.read_feather("/home/share/calypsodata/cooked/20230601/final.feather")
bins = np.arange(-0.1, 1000.2, 0.2)
pressure_interval = pd.cut(df.pressure_x, bins)
df["pressure_interval"] = pressure_interval


if __name__ == "__main__":
    with open("out.log", "r") as f:
        failed = [li.split()[-1] for li in f.readlines() if li.startswith("ERROR")]

    processed = [int(p.stem) for p in Path("cache").glob("*.pkl")]
    print(len(processed))

    to_proc = sorted(list(set(range(len(df))) - set(processed)))
    print(len(to_proc))

    def wrapper_series2data(idx, series):
        if series.donator_x == "debug":
            return 1
        if idx in processed:
            return 1
        try:
            data = series2data(series)
            pickle.dump(data, open(f"cache/{idx}.pkl", "wb"))
            return 0
        except Exception as e:
            return f"ERROR {idx} : {e}"

    res = Parallel(30, verbose=5, backend="multiprocessing")(
        delayed(wrapper_series2data)(idx, ser)
        for idx, ser in df.iloc[to_proc].iterrows()
    )
    print(res)
"""
