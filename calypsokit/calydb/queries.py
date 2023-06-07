from collections import UserDict
from typing import Any

import pymongo
from bson import ObjectId
from pymatgen.core.structure import Structure


# from `calydb/rawcol` to `calydb/uniqcol`
def search_unique():
    pass


class QueryStructure(UserDict):
    def __init__(self, collection):
        """Query and cache pymatgen Structure in this dict

        Only the final structure in the trajectory is returned.

        The key is bson ObjectId.

        Parameters
        ----------
        collection : pymongo.collection.Collection
            pymongo collection to query from.
        """
        super().__init__()
        self.col = collection

    def find_one(self, filter: dict) -> Structure:
        """find one structure

        Parameters
        ----------
        filter : dict
            query filter.

        Returns
        -------
        Structure
            pymatgen structure.

        Raises
        ------
        KeyError
            Cannot find any record in the collection.
        """
        record = self.col.find_one(filter, {"species": 1, "cell": 1, "positions": 1})
        if record is not None:
            if record["_id"] not in self.data:
                structure = Structure(
                    lattice=record["cell"],
                    species=record["species"],
                    coords=record["positions"],
                    coords_are_cartesian=True,
                )
                self.data[record["_id"]] = structure
            else:
                structure = self.data[record["_id"]]
            return structure
        else:
            raise KeyError(f"Cannot find record which satisfy this filter : {filter}")

    def find(self, filter: dict):
        """find many sturctures

        Parameters
        ----------
        filter : dict
            query filter.

        Yields
        ------
        Structure
            pymatgen structure
        """
        cursor = self.col.find(filter, {"species": 1, "cell": 1, "positions": 1})
        for record in cursor:
            if record["_id"] not in self.data:
                structure = Structure(
                    lattice=record["cell"],
                    species=record["species"],
                    coords=record["positions"],
                    coords_are_cartesian=True,
                )
                self.data[record["_id"]] = structure
            else:
                structure = self.data[record["_id"]]
            yield structure

    def __getitem__(self, _id: ObjectId):
        structure = self.data.get(_id, None)
        if structure is None:
            structure = self.find_one({"_id": _id})
        return structure


class QueryTrajectory(UserDict):
    def __init__(self, collection):
        super().__init__()
        self.col = collection

    def find_one(self, filter: dict):
        record = self.col.find_one(filter, {"species": 1, "trajectory": 1})
        if record is not None:
            if record["_id"] not in self.data:
                structures = tuple(
                    Structure(
                        lattice=cell,
                        species=record["species"],
                        coords=positions,
                        coords_are_cartesian=True,
                    )
                    for cell, positions in zip(
                        record["trajectory"]["cell"], record["trajectory"]["positions"]
                    )
                )
                self.data[record["_id"]] = structures
            else:
                structures = self.data[record["_id"]]
            return structures
        else:
            raise KeyError(f"Cannot find record which satisfy this filter : {filter}")

    def find(self, filter: dict):
        cursor = self.col.find(filter, {"species": 1, "cell": 1, "positions": 1})
        for record in cursor:
            if record["_id"] not in self.data:
                structures = tuple(
                    Structure(
                        lattice=cell,
                        species=record["species"],
                        coords=positions,
                        coords_are_cartesian=True,
                    )
                    for cell, positions in zip(
                        record["trajectory"]["cell"], record["trajectory"]["positions"]
                    )
                )
                self.data[record["_id"]] = structures
            else:
                structures = self.data[record["_id"]]
            yield structures

    def __getitem__(self, _id: ObjectId):
        structures = self.data.get(_id, None)
        if structures is None:
            structures = self.find_one({"_id": _id})
        return structures


def pipline_number_in_task(lte: int = -1) -> list[dict[str, Any]]:
    """count number of not deprecated structure in one prediction task, filter by lte

    Parameters
    ----------
    lte : int, optional
        less or equal threshold, no limit if negtive by default -1

    Returns
    -------
    pipline: list[dict[str, Any]]
        pipline list for aggregate, new records with
        {"_id": <source_dir>, "count": int, "ids": [_id, ...]}
    """
    pipline: list[dict[str, Any]] = [
        {"$match": {"deprecated": False}},
        {
            "$group": {
                "_id": "$trajectory.source_dir",
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
            }
        },
    ]
    if lte >= 0:
        pipline.append({"$match": {"count": {"$lte": lte}}})
    return pipline


def pipline_taskgroup() -> list[dict[str, Any]]:
    pipline: list[dict[str, Any]] = [
        {"$match": {"deprecated": False}},
        {
            "$group": {
                "_id": {"task": "$trajectory.source_dir", "formula": "$formula"},
                "ids": {"$push": "$_id"},
                "enth_list": {"$push": "$enthalpy_per_atom"},
            }
        },
    ]
    return pipline


def pipline_sort_enth_by_taskgroup() -> list[dict[str, Any]]:
    """sort enthalpy_per_atom by task group

    Returns
    -------
    pipline: list[dict[str, Any]]
        pipline list for aggregate, new records with
        {"_id": <source_dir>, "sorted_ids": [_id, ...], "sorted_enth": [enth, ...]}
    """
    pipline: list[dict[str, Any]] = [
        {"$match": {"deprecated": False}},
        # group by task, add 'ids' original _id list and 'enth_list'
        {
            "$group": {
                "_id": {"task": "$trajectory.source_dir", "formula": "$formula"},
                "ids": {"$push": "$_id"},
                "enth_list": {"$push": "$enthalpy_per_atom"},
            }
        },
        # zip orginal _id and enthalpy_per_atom together
        {"$addFields": {"zipped": {"$zip": {"inputs": ["$ids", "$enth_list"]}}}},
        # unwind {zipped: [_id, enth]} to each record
        {"$unwind": "$zipped"},
        # sort record by zipped.1 (i.e. enth) and ascending
        {"$sort": {"zipped.1": 1}},
        # group again, push original _id and enth seperately, but keep the relationship
        {
            "$group": {
                "_id": "$_id",
                "sorted_ids": {"$push": {"$arrayElemAt": ["$zipped", 0]}},
                "sorted_enth": {"$push": {"$arrayElemAt": ["$zipped", 1]}},
            }
        },
    ]
    return pipline
