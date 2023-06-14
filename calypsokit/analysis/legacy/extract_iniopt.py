# Extract ini/opt and group the matched one, then insert to rawcol directly
#
# Warning: Do not run this file unless you know what you're doing and have already
# made the proper modification

import pickle
from itertools import chain
from pathlib import Path
from pprint import pprint

import numpy as np
from joblib import Parallel, delayed
from tqdm import tqdm

import calypsokit.analysis.properties as properties
from ase import Atoms
from calypsokit.analysis.legacy.read_inputdat import readinput
from calypsokit.calydb.login import login
from calypsokit.calydb.queries import get_current_caly_max_index
from calypsokit.calydb.record import RecordDict
from calypsokit.analysis.properties import get_cif_str, get_poscar_str

try:
    from calypsokit.analysis.legacy.contactbook import contactbook
except ModuleNotFoundError as e:
    print("ERROR: you do not have the contact book, cannot run this module")
    raise e


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


def get_basic_info(root, results_dir):
    root_date = Path(root).name
    results_dir = Path(results_dir)
    donator = Path(results_dir).relative_to(root).parts[0]
    donator = contactbook[root_date][donator]

    logfile = results_dir.joinpath("CALYPSO.log")
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

    incar_list = sorted(results_dir.parent.glob("INCAR*"))
    potcar = []
    if len(incar_list) == 0:
        # os.system(f'echo "{results_dir}" >> {cwd}/noINCAR-record')
        raise FileNotFoundError(f"{results_dir} No INCAR record")
    else:
        with open(incar_list[-1], "r") as f:
            for line in f:
                if line.strip().startswith("PSTRESS"):
                    pressure = float(line.split("=")[-1]) / 10  # kbar to GPa
                    break
            else:
                raise ValueError(f"No PSTRESS in {incar_list[-1]}")
        with open(incar_list[-1], "r") as f:
            incar = f.read()
    if results_dir.parent.joinpath("POTCAR").exists():
        with open(results_dir.parent.joinpath("POTCAR"), "r") as f:
            for line in f:
                if "TITEL" in line:
                    potcar.append(line.strip())
    # print(donator)
    # print(pressure)
    # print(incar)
    # print(potcar)
    # print(inputdat)
    # print(version)

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
                    positions = scaled_positions @ cell.T
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
                    positions = scaled_positions @ cell.T
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
                print(f"Error extracting structure at line {i + 1}: {str(e)}")
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
                opt_dict[key]["positions"],
                cell=opt_dict[key]["cell"],
                pbc=True,
            )
            opt_dict[key]["cell_abc"] = atoms.cell.lengths().tolist()
            opt_dict[key]["cell_angles"] = atoms.cell.angles().tolist()
            opt_dict[key]["cif"] = get_cif_str(atoms)
            opt_dict[key]["poscar"] = get_poscar_str(atoms)
            opt_dict[key]["min_distance"] = properties.get_min_distance(atoms)
            opt_dict[key]["volume_rate"] = (
                opt_dict[key]["volume"] / opt_dict[key]["clospack_volume"]
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
            print(e)
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


def check_basic_info(root, level=2):
    results_list = get_results_dir(root, level)
    for results in tqdm(results_list):
        return get_basic_info(root, results)


def group_iniopt(root, level=2):
    results_list = get_results_dir(root, level)
    for results in tqdm(results_list):
        basic_info = get_basic_info(root, results)
        ini_dict = extract_ini_structures(root, results, basic_info)
        opt_dict = extract_opt_structures(root, results, basic_info)
        for datadict in match_iniopt(ini_dict, opt_dict, basic_info):
            yield datadict


def create_caly_id(calyidx):
    material_id = f"caly-{calyidx}"
    source = {"name": "calypso", "index": calyidx}
    return material_id, source


def wrapper_insert(idx, datadict):
    material_id, source = create_caly_id(idx)
    try:
        symmetry = properties.wrapped_get_symmetry_from_datadict(datadict)
        datadict["symmetry"] = symmetry
        datadict["material_id"] = material_id
        datadict["source"] = source
        rawrecord = RecordDict(datadict)
        rawrecord.update_time()
    except Exception as e:
        return
    else:
        return rawrecord


if __name__ == "__main__":
    root = "/home/share/calypsodata/raw/20230608"
    root = "/home/share/calypsodata/raw/20230601/debug"
    level = 12
    # for d in get_results_dir(root, level):
    #     print(d)
    # db = login(dotenv_path=".env-maintain")
    # rawcol = db.get_collection("rawcol")
    # cur_caly_max_idx = get_current_caly_max_index(rawcol)
    # print(f"{cur_caly_max_idx=}")

    # -- Check basic info --------------------------------
    # print(check_basic_info(root, level))

    # -- Check get_results_dir  --------------------------------
    # print(sorted(get_results_dir(root, level=8)))

    # -- Check group_iniopt  --------------------------------
    # print(next(group_iniopt(root, level=8)))

    # -- Find and update ---------------------------------
    # cur_caly_max_idx = 0
    # rawrecord_list = Parallel(1, backend="multiprocessing")(
    #     delayed(wrapper_insert)(cur_caly_max_idx + idx + 1, datadict)
    #     for idx, datadict in enumerate(group_iniopt(root, level))
    # )
    # rawrecord_list = [rawcol for rawcol in rawrecord_list if rawcol is not None]
    # print(rawrecord_list)
    # print(len(rawrecord_list))
    # with open(f"{root}/rawrecord.pkl", "wb") as f:
    #     pickle.dump(rawrecord_list, f)
    # rawcol.insert_many(rawrecord_list)

    # -- Insert from saved pkl ---------------------------
    # with open(f"{root}/rawrecord.pkl", "rb") as f:
    #     rawrecord_list = pickle.load(f)
    # rawcol.insert_many(rawrecord_list)
    # ----------------------------------------------------
