# Extract ini/opt and group the matched one, then insert to rawcol directly
#
# Warning: Do not run this file unless you know what you're doing and have already
# made the proper modification

import logging
import pickle
import sys
from itertools import chain
from pathlib import Path
from typing import Union

import numpy as np
from ase import Atoms
from joblib import Parallel, delayed
from tqdm import tqdm

from calypsokit.analysis import properties
from calypsokit.analysis.legacy.read_inputdat import readinput
from calypsokit.calydb.login import login
from calypsokit.calydb.queries import get_current_caly_max_index
from calypsokit.calydb.record import RecordDict

logger = logging.getLogger(__name__)

try:
    from calypsokit.analysis.legacy.contactbook import contactbook
except ModuleNotFoundError as e:
    logger.exception("You do not have the contact book, cannot run this module")
    raise e


def get_results_dir(root, level=1):
    root = Path(root).resolve()
    results_list = []

    def inner_get_dir(path: Path, n: int):
        if n <= 0:
            return
        for deepfile in path.glob("*"):
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


def get_version_from_calylog(calylog):
    if Path(calylog).exists():
        with open(calylog, "r") as log:
            for line in log:
                if line.startswith("Program"):
                    version = line.split()[2]
                    break
            else:
                version = "legacy"
    else:
        version = "legacy"
    return version


def get_pressure_from_incar(incar):
    """read PSTRESS from incar

    Parameters
    ----------
    incar : str
        INCAR file name, str or Path

    Returns
    -------
    pressure: float

    Raises
    ------
    ValueError
        No PSTRESS line
    """
    with open(incar, "r") as f:
        for line in f:
            if line.strip().startswith("PSTRESS"):
                pressure = float(line.split("=")[-1]) / 10  # kbar to GPa
                break
        else:
            pressure = 0.0
            # raise ValueError(f"No PSTRESS in {incar}")
    return pressure


def get_title_from_potcar(potcar):
    potcar_title = []
    with open(potcar, "r") as f:
        for line in f:
            if "TITEL" in line:
                potcar_title.append(line.strip())
    return potcar_title


# root:          /**
# results_dir:   /**/<date>/<name>/**/results*
def get_basic_info(root, results_dir):
    root = Path(root).resolve()
    results_dir = Path(results_dir).resolve()
    root_date, donator, *_ = Path(results_dir).relative_to(root).parts
    if contactbook.get(root_date, None) is None:
        raise KeyError(f"No such root_date: {root_date}")
    elif contactbook[root_date].get(donator, None) is None:
        raise KeyError(f"No such donator: {donator} in root_date: {root_date}")
    else:
        donator = contactbook[root_date][donator]
    # calylog & version
    calylog = results_dir.joinpath("CALYPSO.log")
    version = get_version_from_calylog(calylog)
    # input.dat
    inputdat = readinput(results_dir.parent.joinpath("input.dat"))
    # ICODE
    if inputdat["icode"] == 1:
        # INCAR & pressure
        incar_list = sorted(results_dir.parent.glob("INCAR*"))
        if len(incar_list) == 0:
            # os.system(f'echo "{results_dir}" >> {cwd}/noINCAR-record')
            raise FileNotFoundError(f"{results_dir} No INCAR record")
        else:
            pressure = get_pressure_from_incar(incar_list[-1])
            with open(incar_list[-1], "r") as f:
                dftconfig = f.read()
        # POTCAR
        nelems = len(inputdat["nameofatoms"])
        if results_dir.parent.joinpath("POTCAR").exists():
            pseudopotential = get_title_from_potcar(
                results_dir.parent.joinpath("POTCAR")
            )[:nelems]
        else:
            pseudopotential = ["Not Found"] * nelems
    else:
        raise NotImplementedError("ICODE != 1")

    return (donator, pressure, dftconfig, pseudopotential, inputdat, version)


# root:          /**
# results_dir:   /**/<date>/<name>/**/results*
def extract_ini_structures(root, results_dir, basic_info):
    root = Path(root)
    results_dir = Path(results_dir)
    source_dir = str(results_dir.relative_to(root))  # <date>/<name>/**/results
    donator, pressure, incar, potcar, inputdat, version = basic_info

    atom_names = inputdat["nameofatoms"]

    struct = {}
    for ini_fname in results_dir.rglob("pso_ini_*"):
        source = str(ini_fname.relative_to(root))  # <date>/**/pso_ini_*
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
                    or ''.join(lines[i].strip().split('-')).isalnum()
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
                    positions = scaled_positions @ cell
                    (
                        density,
                        volume,
                        clospack_density,
                        clospack_volume,
                    ) = properties.get_density_clospack_density(species, cell)
                    if volume < 1e-5:
                        raise ValueError("Volume too small")
                    if clospack_volume < 1e-5:
                        raise ValueError("Closepack Volume too small")
                    natoms = int(sum(atom_counts))
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
                logger.error(f"Error extracting structure at line {i + 1}: {str(e)}")

    return struct


def extract_opt_structures(root, results_dir, basic_info):
    root = Path(root)
    results_dir = Path(results_dir)
    source_dir = str(results_dir.relative_to(root))  # <date>/<name>/**/results
    donator, pressure, incar, potcar, inputdat, version = basic_info

    atom_names = inputdat["nameofatoms"]

    struct = {}
    for opt_fname in results_dir.rglob("pso_opt_*"):
        source = str(opt_fname.relative_to(root))  # <date>/**/pso_ini_*
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
                        or ''.join(lines[i].strip().split('-')).isalnum()
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
                    positions = scaled_positions @ cell
                    (
                        density,
                        volume,
                        clospack_density,
                        clospack_volume,
                    ) = properties.get_density_clospack_density(species, cell)
                    if volume < 1e-5:
                        raise ValueError("Volume too small")
                    if clospack_volume < 1e-5:
                        raise ValueError("Closepack Volume too small")
                    natoms = int(sum(atom_counts))
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
                logger.error(f"Error extracting structure at line {i + 1}: {str(e)}")
                continue
    return struct


def match_iniopt(ini_dict, opt_dict, basic_info):
    donator, pressure, incar, potcar, inputdat, version = basic_info
    pressure_range = properties.get_pressure_range(pressure)
    matched_keys = list(set(ini_dict.keys()) & set(opt_dict.keys()))
    for key in matched_keys:
        try:
            assert ini_dict[key]["volume"] > 1e-5, "ini volume too small"
            assert opt_dict[key]["volume"] > 1e-5, "opt volume too small"
            assert ini_dict[key]["natoms"] == opt_dict[key]["natoms"], "natoms unmatch"
            formula, reduced_formula = properties.get_formula(ini_dict[key]["species"])
            trajectory = {
                "nframes": 2,
                "cell": np.stack([d["cell"] for d in [ini_dict[key], opt_dict[key]]]),
                "positions": np.stack(
                    [d["positions"] for d in [ini_dict[key], opt_dict[key]]]
                ),
                "scaled_positions": np.stack(
                    [d["scaled_positions"] for d in [ini_dict[key], opt_dict[key]]]
                ),
                "forces": np.stack(
                    [d["forces"] for d in [ini_dict[key], opt_dict[key]]]
                ),
                "volume": [d["volume"] for d in [ini_dict[key], opt_dict[key]]],
                "volume_per_atom": [
                    d["volume_per_atom"] for d in [ini_dict[key], opt_dict[key]]
                ],
                "enthalpy": [d["enthalpy"] for d in [ini_dict[key], opt_dict[key]]],
                "enthalpy_per_atom": [
                    d["enthalpy_per_atom"] for d in [ini_dict[key], opt_dict[key]]
                ],
                "source_file": [
                    d["source_file"] for d in [ini_dict[key], opt_dict[key]]
                ],
                "source_idx": [d["source_idx"] for d in [ini_dict[key], opt_dict[key]]],
                "source_dir": opt_dict[key]["source_dir"],
            }
            atoms = Atoms(
                opt_dict[key]["species"],
                scaled_positions=opt_dict[key]["scaled_positions"],
                cell=opt_dict[key]["cell"],
                pbc=True,
            )
            opt_dict[key]["cell_abc"] = atoms.cell.lengths().tolist()
            opt_dict[key]["cell_angles"] = atoms.cell.angles().tolist()
            opt_dict[key]["cif"] = properties.get_cif_str(atoms)
            opt_dict[key]["poscar"] = properties.get_poscar_str(atoms)
            opt_dict[key]["min_distance"] = properties.get_min_distance(atoms)
            opt_dict[key]["volume_rate"] = (
                opt_dict[key]["volume"] / opt_dict[key]["clospack_volume"]
            )
            opt_dict[key]["clospack_volume_per_atom"] = (
                opt_dict[key]["clospack_volume"] / opt_dict[key]["natoms"]
            )
            # ---------------------------------------------------------------------
            trajectory["kabsch"] = properties.get_kabsch_info(
                ini_dict[key]["cell"], opt_dict[key]["cell"]
            )
            trajectory["shifted_d_frac"] = properties.get_shifted_d_frac(
                ini_dict[key]["scaled_positions"], opt_dict[key]["scaled_positions"]
            )
            trajectory["strain"] = properties.get_strain_info(
                ini_dict[key]["cell"], opt_dict[key]["cell"]
            )
            # ---------------------------------------------------------------------
        except Exception as e:
            logger.exception(f"{e}")
            continue
        else:
            data = {
                key: val
                for key, val in opt_dict[key].items()
                if key not in ["source_dir", "source_file", "source_idx"]
            }
            data.update(
                {
                    "formula": formula,
                    "reduced_formula": reduced_formula,
                    "pressure": pressure,
                    "pressure_range": pressure_range,
                    "trajectory": trajectory,
                    "calyconfig": {"version": version, **inputdat},
                    "dftconfig": [incar],
                    "pseudopotential": potcar,
                    "donator": donator,
                    "deprecated": False,
                    "deprecated_reason": "",
                }
            )
            yield data


class GroupIniOpt:
    def __init__(self, root, results_list=[]):
        """Group all ini-opt from results_list

        Parameters
        ----------
        root : Path | str
            top level root dir which will be removed from results dir
        results_list : list, optional, default []
            results* dir list, all should be <root>/<date>/<name>/**/results*
        """
        self.root = root
        self.results_list = results_list

    def check_basic_info(self):
        for results in tqdm(self.results_list):
            try:
                get_basic_info(self.root, results)
            except Exception as e:
                logger.exception(f"{results} failed : {e}")
            else:
                yield results

    def group_one_results(self, results: Union[Path, str]):
        basic_info = get_basic_info(self.root, results)
        ini_dict = extract_ini_structures(self.root, results, basic_info)
        opt_dict = extract_opt_structures(self.root, results, basic_info)
        for datadict in match_iniopt(ini_dict, opt_dict, basic_info):
            yield datadict

    def wrapped_group_one_results(self, results: Union[Path, str]):
        return list(self.group_one_results(results))

    def __call__(self, j=-1):
        datadict_each_dir = Parallel(j, backend='multiprocessing')(
            delayed(self.wrapped_group_one_results)(results)
            for results in self.check_basic_info()
        )
        datadict_list = list(chain.from_iterable(datadict_each_dir))
        return datadict_list

    @classmethod
    def from_file(cls, root, results_list_file):
        with open(results_list_file, 'r') as f:
            results_list = [line.strip() for line in f.readlines()]
        return cls(root, results_list)

    @classmethod
    def from_recursive(cls, root, level=1):
        results_list = get_results_dir(root, level)
        return cls(root, results_list)


def create_caly_id(calyidx):
    material_id = f"caly-{calyidx}"
    source = {"name": "calypso", "index": calyidx}
    return material_id, source


def patch_before_insert(calyidx, datadict):
    material_id, source = create_caly_id(calyidx)
    symmetry = properties.wrapped_get_symmetry_from_datadict(datadict)
    datadict["symmetry"] = symmetry
    # datadict["material_id"] = material_id
    datadict["source"] = source
    rawrecord = RecordDict(datadict)
    rawrecord.update_time()
    return rawrecord


def wrapper_patch_before_insert(calyidx, datadict):
    try:
        rawrecord = patch_before_insert(calyidx, datadict)
    except Exception as e:
        logger.exception(f"{e}")
        return
    else:
        return rawrecord


if __name__ == "__main__":
    pass
    # -- Insert from saved pkl ---------------------------
    # db = login(dotenv_path=".env-maintain")
    # raw = db.get_collection('raw')
    # cur_caly_max_idx = get_current_caly_max_index(raw)
    # cur_caly_max_idx = 1107480
    # data_file = Path(sys.argv[1])
    # with open(data_file, "rb") as f:
    #     datadict_list = pickle.load(f)
    # rawrecord_list = Parallel(30, backend="multiprocessing")(
    #     delayed(wrapper_patch_before_insert)(calyidx, datadict)
    #     for calyidx, datadict in tqdm(enumerate(datadict_list, cur_caly_max_idx + 1))
    # )
    # rawrecord_list = [record for record in rawrecord_list if record is not None]
    # # rawrecord_list = pickle.load(open(sys.argv[1], 'rb'))
    # raw.insert_many(rawrecord_list)
    # ----------------------------------------------------
