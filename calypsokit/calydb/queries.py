import logging

from collections import UserDict
from datetime import datetime
from typing import Any, Optional

from ase import Atoms
from bson import ObjectId
from pymatgen.core.structure import Structure


logger = logging.getLogger(__name__)


class QueryStructure(UserDict):
    """Query and cache structure or trajectory in this dict

    Item as {ObjectId: {"_id": ..., "_structure_": ..., ...}}.
    The key is bson ObjectId.

    Call QueryStructure(...)[<_id>] will first find the cached dict, then call
    find_one(_id), return None if nothing is found.

    Examples
    --------
    Only query structure and output pymatgen-type
    >>> col: collection
    >>> qs = QueryStructure(col)
    >>> qs.find_one()
    {..., "_structure_": <one pmg structure>}

    Query and output as Generator
    >>> for item in qs.find({}):
    ...     print(item)
    ...     break
    {..., "_structure_": <one pmg structure>}

    Query structure and other properties, output ase-type
    >>> col: collection
    >>> projection = {"enthalpy_per_atom": 1}
    >>> qs = QueryStructure(col, projection, type="ase")
    >>> qs.find_one()
    {..., "_structure_": <one ase structure>, "enthalpy_per_atom": <enth>}

    Query trajectory and other properties, output ase-type list
    >>> col: collection
    >>> projection = {"enthalpy_per_atom": 1}
    >>> qs = QueryStructure(col, projection, trajectory=True, type="ase")
    >>> qs.find_one()
    {..., "_structure_": <list of ase structures>, "enthalpy_per_atom": <enth>}

    """

    def __init__(self, collection, projection={}, trajectory=False, type="pmg"):
        """Init this dict

        Parameters
        ----------
        collection : pymongo.collection.Collection
            pymongo collection to query from.
        projection: Optional[dict]
            query all info to properties if None, otherwise query the given projection
        trajectory: Optional[bool], default False
            return trajectory structure list instead
        type: {'pmg', 'ase'}, default 'pmg'
            type of returned structure, 'pmg' for pymatgen Structure,
            'ase' for ase Atoms
        """
        super().__init__()
        self.col = collection
        if projection is None:
            self.projection = {}
        elif isinstance(projection, dict):
            self.projection = projection | {
                "_id": 1,
                "species": 1,
                "cell": 1,
                "positions": 1,
            }
            if trajectory:
                self.projection["trajectory"] = 1
        else:
            raise ValueError("projection must be a dict or None")
        self.trajectory = trajectory
        self.type = type

    def find_one(self, filter: dict = {}):
        """find one structure

        Parameters
        ----------
        filter : dict
            query filter.

        Returns
        -------
        record : dict or None, {..., "_structure_": <one structure or list>}
        """
        record = self.col.find_one(filter, self.projection)
        if record is not None:
            if record["_id"] not in self.data:
                record["_structure_"] = self.record2structure(record)
                self.data[record["_id"]] = record
            return self.data[record["_id"]]
        else:
            return None

    def find(self, filter: dict):
        """find many sturctures

        Parameters
        ----------
        filter : dict
            query filter.

        Yields
        ------
        record : dict or None, {..., "_structure_": <one structure or list>}
        """
        cursor = self.col.find(filter, self.projection)
        for record in cursor:
            if record["_id"] not in self.data:
                record["_structure_"] = self.record2structure(record)
                self.data[record["_id"]] = record
            yield self.data[record["_id"]]

    def __getitem__(self, _id: ObjectId):
        item = self.data.get(_id, None)
        if item is None:
            item = self.find_one({"_id": _id})
        return item

    def record2structure(self, record):
        if self.trajectory:
            species = record["species"]
            traj_cell = record["trajectory"]["cell"]
            traj_pos = record["trajectory"]["positions"]
            if self.type == "pmg":
                trajectory = [
                    self._build_pmg(species, cell, positions)
                    for cell, positions in zip(traj_cell, traj_pos)
                ]
                return trajectory
            elif self.type == "ase":
                trajectory = [
                    self._build_ase(species, cell, positions)
                    for cell, positions in zip(traj_cell, traj_pos)
                ]
                return trajectory
        else:
            species = record["species"]
            cell = record["cell"]
            positions = record["positions"]
            if self.type == "pmg":
                return self._build_pmg(species, cell, positions)
            elif self.type == "ase":
                return self._build_ase(species, cell, positions)

    def _build_pmg(self, species, cell, positions):
        structure = Structure(cell, species, positions, coords_are_cartesian=True)
        return structure

    def _build_ase(self, species, cell, positions):
        structure = Atoms(symbols=species, cell=cell, positions=positions, pbc=True)
        return structure


class Pipes:
    @staticmethod
    def undeprecated_record():
        pipeline = [{"$match": {"deprecated": False}}]
        return pipeline

    @staticmethod
    def daterange_records(mindate=None, maxdate=None):
        if mindate is None:
            mindate = (1, 1, 1, 0, 0, 0)
        if maxdate is None:
            maxdate = (9999, 12, 31, 23, 59, 59)
        pipeline = [
            {
                "$match": {
                    "last_updated_utc": {
                        "$gt": datetime(*mindate),
                        "$lt": datetime(*maxdate),
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def get_edge_time(side):
        if side == "max":
            seq = -1
        elif side == "min":
            seq = 1
        else:
            raise ValueError("side must be 'max' or 'min'")
        pipeline = [
            {'$sort': {'last_updated_utc': seq}},
            {'$limit': 1},
            {'$project': {'last_updated_utc': 1}},
        ]
        return pipeline

    @staticmethod
    def group_task(*, lte: int = -1) -> list[dict[str, Any]]:
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
            # {"$match": {"source.name": "calypso"}},
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

    @staticmethod
    def group_task_formula() -> list[dict[str, Any]]:
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

    @staticmethod
    def sort_enthalpy() -> list[dict[str, Any]]:
        """sort enthalpy_per_atom by group of task and formula

        Returns
        -------
        pipline: list[dict[str, Any]]
            pipline list for aggregate, new records with
            {"_id": <source_dir>, "sorted_ids": [_id, ...], "sorted_enth": [enth, ...]}
        """
        pipeline: list[dict[str, Any]] = Pipes.group_task_formula() + [
            # zip orginal _id and enthalpy_per_atom together
            {"$addFields": {"zipped": {"$zip": {"inputs": ["$ids", "$enth_list"]}}}},
            # unwind {zipped: [_id, enth]} to each record
            {"$unwind": "$zipped"},
            # sort record by zipped.1 (i.e. enth) and ascending
            {"$sort": {"zipped.1": 1}},
            # group again, push original _id and enth seperately, but keep related
            {
                "$group": {
                    "_id": "$_id",
                    "sorted_ids": {"$push": {"$arrayElemAt": ["$zipped", 0]}},
                    "sorted_enth": {"$push": {"$arrayElemAt": ["$zipped", 1]}},
                }
            },
        ]
        return pipeline

    @staticmethod
    def unique_records(uniqcol="uniq"):
        pipeline = [
            {
                "$lookup": {
                    "from": f"{uniqcol}",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "intersection",
                }
            },
            {"$match": {"intersection": {"$ne": []}}},
            {"$unwind": "$intersection"},
        ]
        return pipeline

    @staticmethod
    def cdvae_records(uniqcol="uniqcol"):
        pipeline = [
            {
                "$match": {
                    "deprecated": False,
                    "min_distance": {"$gte": 0.5, "$lte": 5.2},
                    "volume_rate": {"$lte": 4},
                }
            },
        ] + Pipes.unique_records(uniqcol)
        return pipeline

    @staticmethod
    def check_duplicate():
        pipeline = [
            # {"$limit": 100},
            {
                "$group": {
                    "_id": {
                        "file": {"$arrayElemAt": ["$trajectory.source_file", 0]},
                        "idx": {"$arrayElemAt": ["$trajectory.source_idx", 0]},
                    },
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
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


def get_edge_time(collection, side):
    cursor = list(collection.aggregate(Pipes.get_newest_time(side)))
    if len(cursor) == 0:
        return (1, 1, 1, 0, 0, 0)
    else:
        t = cursor[0]["last_updated_utc"]
        return (t.year, t.month, t.day, t.hour, t.minute, t.second)


def check_duplicate(collection):
    cur = list(collection.aggregate(Pipes.check_duplicate()))
    for rec in cur:
        str_ids = " ".join(map(str, rec["ids"]))
        logger.info(f"{len(rec)} duplicates, _id list {str_ids}")
    else:
        logger.info("No duplicates")
    return cur


def delete_duplicates(collection):
    cur = check_duplicate(collection)
    collection.delete_many({"_id": {"$in": [rec["ids"][0] for rec in cur]}})
