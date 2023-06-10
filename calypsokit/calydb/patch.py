import numpy as np

import calypsokit.analysis.properties as properties
from calypsokit.calydb.queries import QueryStructure


class RawRecordPatcher:
    def __init__(self, rawcol, force_update=[]):
        self.rawcol = rawcol
        self.qs = QueryStructure(rawcol, None, trajectory=True, type="pmg")
        self.force_update = force_update

    def patch(self, _id):
        update_dict = self.get_update_dict(_id)
        self.rawcol.update_one({"_id": _id}, {"$set": update_dict})

    def get_update_dict(self, _id):
        record = self.qs.find_one({"_id": _id})
        if record is None:
            raise ValueError(f"{_id=} not found in collection {self.rawcol.name}")
        update_dict = {}
        trajectory = record["_structure_"]
        final_frame = trajectory[-1]
        # Patch cell par
        if "cell_abc" not in record or "cell_abc" in self.force_update:
            cell_abc = list(final_frame.lattice.abc)
            update_dict["cell_abc"] = cell_abc
        if "cell_angles" not in record or "cell_angles" in self.force_update:
            cell_angles = list(final_frame.lattice.angles)
            update_dict["cell_angles"] = cell_angles
        # Patch volume_per_atom and volume_rate
        if "volume_per_atom" not in record or "volume_rate" in self.force_update:
            update_dict["volume_per_atom"] = record["volume"] / record["natoms"]
        if "volume_rate" not in record or "volume_rate" in self.force_update:
            update_dict["volume_rate"] = record["volume"] / record["clospack_volume"]
        # Patch minmum distance
        if "min_distance" not in record or "min_distance" in self.force_update:
            update_dict["min_distance"] = properties.get_min_distance(final_frame)
        # Patch dimension
        if "dim_larsen" not in record or "dim_larsen" in self.force_update:
            dim_larsen = properties.get_dim_larsen(final_frame)
            update_dict["dim_larsen"] = dim_larsen
        # Patch kabsch
        if (
            "kabsch" not in record["trajectory"]
            or "trajectory.kabsch" in self.force_update
        ):
            celli = record["trajectory"]["cell"][0]
            cellr = record["trajectory"]["cell"][-1]
            kabsch_info = properties.get_kabsch_info(celli, cellr)
            update_dict["trajectory.kabsch"] = kabsch_info
        # Patch shifted_d_frac, shifted delta fractional coordinates
        if (
            "shifted_d_frac" not in record["trajectory"]
            or "shifted_d_frac" in self.force_update
        ):
            fraci = record["trajectory"]["scaled_positions"][0]
            fracr = record["trajectory"]["scaled_positions"][-1]
            shifted_d_frac_info = properties.get_shifted_d_frac(fraci, fracr)
            update_dict["trajectory.shifted_d_frac"] = shifted_d_frac_info
        # Patch strain
        if (
            "strain" not in record["trajectory"]
            or "trajectory.strain" in self.force_update
        ):
            celli = record["trajectory"]["cell"][0]
            cellr = record["trajectory"]["cell"][-1]
            strain_info = properties.get_strain_info(celli, cellr)
            update_dict["trajectory.strain"] = strain_info
        return update_dict
