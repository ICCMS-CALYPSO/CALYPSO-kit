from joblib import Parallel, delayed
from tqdm import tqdm

from calypsokit.analysis import properties
from calypsokit.calydb.login import login
from calypsokit.calydb.queries import QueryStructure


class RawRecordPatcher:
    def __init__(self, rawcol):
        self.rawcol = rawcol
        self.qs = QueryStructure(rawcol, None, trajectory=True, type="pmg")

    def parallel_patch_cell_abc(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "cell_abc": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_cell_abc)(_id) for _id in tqdm(_id_list)
        )

    def _patch_cell_abc(self, _id):
        record = self.qs.find_one({"_id": _id})
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        cell_abc = list(final_frame.lattice.abc)
        self.rawcol.update_one({"_id": _id}, {"$set": {"cell_abc": cell_abc}})

    def parallel_patch_cell_angles(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "cell_angles": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_cell_angles)(_id) for _id in tqdm(_id_list)
        )

    def _patch_cell_angles(self, _id):
        record = self.qs.find_one({"_id": _id})
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        cell_angles = list(final_frame.lattice.angles)
        self.rawcol.update_one({"_id": _id}, {"$set": {"cell_angles": cell_angles}})

    def parallel_patch_volume_per_atom(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "volume_per_atom": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_volume_per_atom)(_id) for _id in tqdm(_id_list)
        )

    def _patch_volume_per_atom(self, _id):
        record = self.qs.find_one({"_id": _id})
        volume_per_atom = record["volume"] / record["natoms"]
        self.rawcol.update_one(
            {"_id": _id},
            {"$set": {"volume_per_atom": volume_per_atom}},
        )

    def parallel_patch_volume_rate(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "volume_rate": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_volume_rate)(_id) for _id in tqdm(_id_list)
        )

    def _patch_volume_rate(self, _id):
        record = self.qs.find_one({"_id": _id})
        self.rawcol.update_one(
            {"_id": _id},
            {"$set": {"volume_rate": record["volume"] / record["clospack_volume"]}},
        )

    def parallel_patch_min_distance(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "min_distance": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_min_distance)(_id) for _id in tqdm(_id_list)
        )

    def _patch_min_distance(self, _id):
        record = self.qs.find_one({"_id": _id})
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        min_distance = properties.get_min_distance(final_frame)
        self.rawcol.update_one({"_id": _id}, {"$set": {"min_distance": min_distance}})

    def parallel_patch_dim_larsen(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "dim_larsen": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_dim_larsen)(_id) for _id in tqdm(_id_list)
        )

    def _patch_dim_larsen(self, _id):
        record = self.qs.find_one({"_id": _id})
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        dim_larsen = properties.get_dim_larsen(final_frame)
        self.rawcol.update_one({"_id": _id}, {"$set": {"dim_larsen": dim_larsen}})

    def parallel_patch_kabsch(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "trajectory.kabsch": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_kabsch)(_id) for _id in tqdm(_id_list)
        )

    def _patch_kabsch(self, _id):
        record = self.qs.find_one({"_id": _id})
        celli = record["trajectory"]["cell"][0]
        cellr = record["trajectory"]["cell"][-1]
        kabsch_info = properties.get_kabsch_info(celli, cellr)
        self.rawcol.update_one(
            {"_id": _id},
            {"$set": {"trajectory.kabsch": kabsch_info}},
        )

    def parallel_patch_shifted_d_frac(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "trajectory.shifted_d_frac": {"$exists": False}},
            {"_id": 1},
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_shifted_d_frac)(_id) for _id in tqdm(_id_list)
        )

    def _patch_shifted_d_frac(self, _id):
        record = self.qs.find_one({"_id": _id})
        fraci = record["trajectory"]["scaled_positions"][0]
        fracr = record["trajectory"]["scaled_positions"][-1]
        shifted_d_frac_info = properties.get_shifted_d_frac(fraci, fracr)
        self.rawcol.update_one(
            {"_id": _id},
            {"$set": {"trajectory.shifted_d_frac": shifted_d_frac_info}},
        )

    def parallel_patch_strain(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "trajectory.strain": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_strain)(_id) for _id in tqdm(_id_list)
        )

    def _patch_strain(self, _id):
        record = self.qs.find_one({"_id": _id})
        celli = record["trajectory"]["cell"][0]
        cellr = record["trajectory"]["cell"][-1]
        strain_info = properties.get_strain_info(celli, cellr)
        self.rawcol.update_one(
            {"_id": _id},
            {"$set": {"trajectory.strain": strain_info}},
        )

    def parallel_patch_cif(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "cif": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_cif)(_id) for _id in tqdm(_id_list)
        )

    def _patch_cif(self, _id):
        record = self.qs.find_one({"_id": _id})
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        cif = properties.get_cif_str(final_frame)
        self.rawcol.update_one({"_id": _id}, {"$set": {"cif": cif}})

    def parallel_patch_poscar(self):
        cursor = self.rawcol.find(
            {"deprecated": False, "poscar": {"$exists": False}}, {"_id": 1}
        )
        _id_list = [record["_id"] for record in cursor]
        Parallel(backend="multiprocessing")(
            delayed(self._patch_poscar)(_id) for _id in tqdm(_id_list)
        )

    def _patch_poscar(self, _id):
        record = self.qs.find_one({"_id": _id})
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        poscar = properties.get_poscar_str(final_frame)
        self.rawcol.update_one({"_id": _id}, {"$set": {"poscar": poscar}})


if __name__ == "__main__":
    db = login()
    rawcol = db.get_collection("rawcol")
    patcher = RawRecordPatcher(rawcol)
    # print("patching min_distance")
    # patcher.parallel_patch_min_distance()
    # print("patching volume_rate")
    # patcher.parallel_patch_volume_rate()
    # ------------------------------------
    # print("patching dim_larsen")
    # patcher.parallel_patch_dim_larsen()
    # ------------------------------------
    # print("patching cell_abc")
    # patcher.parallel_patch_cell_abc()
    # print("patching cell_angles")
    # patcher.parallel_patch_cell_angles()
    # print("patching volume_per_atom")
    # patcher.parallel_patch_volume_per_atom()
    # print("patching kabsch")
    # patcher.parallel_patch_kabsch()
    # print("patching shifted_d_frac")
    # patcher.parallel_patch_shifted_d_frac()
    # print("patching strain")
    # patcher.parallel_patch_strain()
    # ------------------------------------
    # print("patching cif string")
    # patcher.parallel_patch_cif()
    # print("patching poscar string")
    # patcher.parallel_patch_poscar()
