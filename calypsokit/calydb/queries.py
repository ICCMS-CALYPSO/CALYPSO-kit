from collections import UserDict
from typing import Any

from bson import ObjectId
from pymatgen.core.structure import Structure


class QueryStructure(UserDict):
    def __init__(self, collection, projection: dict = {}):
        """Query and cache pymatgen Structure in this dict

        i.e. same as the final structure in the trajectory.

        Item as {ObjectId: (Structure, properties)}. The key is bson ObjectId.

        Parameters
        ----------
        collection : pymongo.collection.Collection
            pymongo collection to query from.
        projection: dict

        """
        super().__init__()
        self.col = collection
        self.projection = projection | {
            "_id": 1,
            "species": 1,
            "cell": 1,
            "positions": 1,
        }

    def find_one(self, filter: dict):
        """find one structure

        Parameters
        ----------
        filter : dict
            query filter.

        Returns
        -------
        structure: Structure
        properties: dict

        Raises
        ------
        KeyError
            Cannot find any record in the collection.
        """
        record = self.col.find_one(filter, self.projection)
        if record is not None:
            if record["_id"] not in self.data:
                lattice = record.pop("cell")
                species = record.pop("species")
                coords = record.pop("positions")
                structure = Structure(
                    lattice=lattice,
                    species=species,
                    coords=coords,
                    coords_are_cartesian=True,
                )
                self.data[record["_id"]] = (structure, record)
            return self.data[record["_id"]]
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
        structure: Structure
        properties: dict
        """
        cursor = self.col.find(filter, self.projection)
        for record in cursor:
            if record["_id"] not in self.data:
                lattice = record.pop("cell")
                species = record.pop("species")
                coords = record.pop("positions")
                structure = Structure(
                    lattice=lattice,
                    species=species,
                    coords=coords,
                    coords_are_cartesian=True,
                )
                self.data[record["_id"]] = (structure, record)
            yield self.data[record["_id"]]

    def __getitem__(self, _id: ObjectId):
        item = self.data.get(_id, None)
        if item is None:
            item = self.find_one({"_id": _id})
        return item


class QueryTrajectory(UserDict):
    def __init__(self, collection, projection: dict = {}):
        super().__init__()
        self.col = collection
        self.projection = projection | {"_id": 1, "species": 1, "trajectory": 1}

    def find_one(self, filter: dict):
        record = self.col.find_one(filter, self.projection)
        if record is not None:
            if record["_id"] not in self.data:
                species = record.pop("species")
                traj_cell = record["trajectory"].pop("cell")
                traj_pos = record["trajectory"].pop("positions")
                trajectory = tuple(
                    Structure(
                        lattice=cell,
                        species=species,
                        coords=positions,
                        coords_are_cartesian=True,
                    )
                    for cell, positions in zip(traj_cell, traj_pos)
                )
                self.data[record["_id"]] = (trajectory, record)
            return self.data[record["_id"]]
        else:
            raise KeyError(f"Cannot find record which satisfy this filter : {filter}")

    def find(self, filter: dict):
        cursor = self.col.find(filter, self.projection)
        for record in cursor:
            if record["_id"] not in self.data:
                species = record.pop("species")
                traj_cell = record["trajectory"].pop("cell")
                traj_pos = record["trajectory"].pop("positions")
                trajectory = tuple(
                    Structure(
                        lattice=cell,
                        species=species,
                        coords=positions,
                        coords_are_cartesian=True,
                    )
                    for cell, positions in zip(traj_cell, traj_pos)
                )
                self.data[record["_id"]] = (trajectory, record)
            yield self.data[record["_id"]]

    def __getitem__(self, _id: ObjectId):
        item = self.data.get(_id, None)
        if item is None:
            item = self.find_one({"_id": _id})
        return item


def pipe_undeprecated_record():
    pipeline = [{"$match": {"deprecated": False}}]
    return pipeline


def pipe_group_task(lte: int = -1) -> list[dict[str, Any]]:
    """Group task by trajectory.source_dir, then count number of not deprecated
    structure, filter by lte.

    Remember: One task may has more than one formual (VSC task).

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
    pipeline: list[dict[str, Any]] = [
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
        pipeline.append({"$match": {"count": {"$lte": lte}}})
    return pipeline


def pipe_group_task_formula() -> list[dict[str, Any]]:
    """Group by task and formula"""
    pipeline: list[dict[str, Any]] = [
        {"$match": {"deprecated": False}},
        {
            "$group": {
                "_id": {"task": "$trajectory.source_dir", "formula": "$formula"},
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
                "enth_list": {"$push": "$enthalpy_per_atom"},
            }
        },
    ]
    return pipeline


def pipe_sort_enthalpy() -> list[dict[str, Any]]:
    """sort enthalpy_per_atom by group of task and formula

    Returns
    -------
    pipline: list[dict[str, Any]]
        pipline list for aggregate, new records with
        {"_id": <source_dir>, "sorted_ids": [_id, ...], "sorted_enth": [enth, ...]}
    """
    pipeline: list[dict[str, Any]] = [
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
    return pipeline


def pipe_unique_records(fromcol="rawcol"):
    pipeline = [
        {
            '$lookup': {
                'from': f"{fromcol}",
                'localField': '_id',
                'foreignField': '_id',
                'as': 'matched_docs',
            }
        },
        # No neet to Filter the documents with matches, cause unwind will not output
        # empty list by default
        # {'$match': {'matched_docs': {'$ne': []}}},
        {"$unwind": "$matched_docs"},
        {
            "$replaceRoot": {
                "newRoot": {"$mergeObjects": [{"version": "$version"}, "$matched_docs"]}
            }
        },
        {"$match": {"deprecated": False}},
    ]
    return pipeline


def get_current_caly_max_index(collection) -> int:
    """get max calypso index in the collection

    max `source.index` in which `source.name == calypso`

    Parameters
    ----------
    collection : collection
        collection

    Returns
    -------
    int
        current max index
    """
    cur = collection.find({"source.name": "calypso"}).sort("source.index", -1).limit(1)
    cur = list(cur)
    if len(cur) == 0:
        return 0
    else:
        return cur[0]["source"]["index"]
