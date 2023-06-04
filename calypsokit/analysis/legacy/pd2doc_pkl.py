import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from ase import Atoms
from ase.data import atomic_masses, atomic_numbers, covalent_radii
from ase.spacegroup import get_spacegroup
from joblib import Parallel, delayed
from read_inputdat import readinput
from tqdm import tqdm

from calypsokit.calydb.login import login

df = pd.read_feather("/home/share/calypsodata/cooked/20230601/final.feather")
donator_dict = {
    "chenxin": "Chen Xin",
    "kouchunlei": "Kou Chunlei",
    "liupeng": "Liu Peng",
    "pengfeng": "Peng Feng",
    "wangyanchao": "Wang Yanchao",
    "wangzhenyu": "Wang Zhenyu",
    "xumeiling": "Xu Meiling",
    "zhangdandan": "Zhang Dandan",
    "zhangshoutao": "Zhang Shoutao",
    "cuixiangyue": "Cui Xiangyue",
    "gaopengyue": "Gao Pengyue",
    "lihonglin": "Li Honglin",
    "lucheng": "Lu Cheng",
    "quxin": "Qu Xin",
    "xiezhuohang": "Xie Zhuohang",
    "yangguochun": "Yang Guochun",
    "zhangxinyu": "Zhang Xinyu",
}
bins = np.arange(-0.1, 1000.2, 0.2)
pressure_interval = pd.cut(df.pressure_x, bins)
df["pressure_interval"] = pressure_interval


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


def get_density_clospack_density(atoms):
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
    clospack_density = mass / clospack_volume  # (g/molA3)  * 6.02 * 1e-27 * 1e30(g/m3)
    return density, volume, clospack_volume, clospack_density


def series2data(series):
    formula = "".join(f"{e}{n}" for e, n in zip(series.atom_names_ini, series.atom_counts_ini))
    if len(list(series.coord_ini)) != len(list(series.coord_opt)):
        raise ValueError("number of atoms not match")
    atoms_ini = Atoms(formula, cell=list(series.lattice_ini), scaled_positions=list(series.coord_ini), pbc=True)
    atoms_opt = Atoms(formula, cell=list(series.lattice_opt), scaled_positions=list(series.coord_opt), pbc=True)
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
    opt_density, volume, clospack_volume, clospack_density = get_density_clospack_density(atoms_opt)
    calyconfig = {"version": parse_version(series)}
    try:
        calyconfig.update(readinput(input_str=series.inputdat_x))
    except Exception as e:
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
            "scaled_positions": np.stack([atoms_ini.get_scaled_positions(), atoms_opt.get_scaled_positions()]),
            "forces": np.zeros([2, len(atoms_ini), 3]) * np.nan,
            "volume": [atoms_ini.get_volume(), atoms_opt.get_volume()],
            "enthalpy": [np.nan, series.enthalpy_per_atom * len(atoms_ini)],
            "enthalpy_per_atom": [np.nan, series.enthalpy_per_atom],
            "source": [ini_path, opt_path],
            "source_idx": [ini_idx, opt_idx],
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

processed = [p.stem for p in Path("cache").glob("*.pkl")]
with open("out.log", "r") as f:
    failed = [l.split()[-1] for l in f.readlines() if l.startswith("ERROR")]


processed = [int(p.stem) for p in Path("cache").glob("*.pkl")]
print(len(processed))

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


to_proc = sorted(list(set(range(len(df))) - set(processed)))
print(len(to_proc))


if __name__ == "__main__":
    res = Parallel(30, verbose=5, backend="multiprocessing")(
        delayed(wrapper_series2data)(idx, ser) for idx, ser in df.iloc[to_proc].iterrows()
    )
    print(res)

    # with open("fail_uniq-1.log", "a", 1) as f:
    #     for idx, ser in tqdm(df.iloc[to_proc].iterrows()):
    #         # 504218 504466 504487 504499 crash
    #         if idx <= 504499:
    #             continue
    #         errlog = wrapper_series2data(idx, ser)
    #         if isinstance(errlog, str):
    #             f.write(errlog + "\n")
    #         elif errlog == 0:
    #             print(idx)