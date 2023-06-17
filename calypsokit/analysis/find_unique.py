# Find the unique structure in calypso data
# DO NOT run this file directly unless you know what you are doing!
# This file may need modification for more specific unique-finding method


from datetime import datetime
from functools import lru_cache
from itertools import chain

import pymongo
from joblib import Parallel, delayed
from pymatgen.analysis.structure_matcher import StructureMatcher
from tqdm import tqdm

from calypsokit.calydb.login import login
from calypsokit.calydb.queries import Pipes, QueryStructure


class UniqueFinder:
    def __init__(self, rawcol, uniqcol, e_threshold=0.005, match_kwargs={}):
        """_summary_

        Determine unique by enthalpy_per_atom threshold 5meV (default) and pymatgen
        StructureMatcher method match or not.

        1. Group structures by task and formula.
        2. For each structure in on group, one is compared to every picked structures.
        3. If this two structures have the same space group (1e-1) and energy differs
        less than 5meV, then they need to *match*. Otherwise they are considered to
        be different and pick the one directly.
        4. If matched, they are the same, the lower energy one will replace higher one.
        5. If not matched, this one is added to the picked structures.
        6. Finally the picked structures are the uniqe structures.

        Examples
        --------
        >>> rawcol: rawcol
        >>> uniqcol: uniqcol
        >>> uniquefinder = UniqueFinder(rawcol, uniqcol)
        >>> mindate = (2023, 6, 8)
        >>> uniquefinder.update(mindate, version="*")

        for debug

        >>> for uniq_list in uniquefinder.check(mindate):
        ...     print(uniq_list)
        [ObjectId('...'), ...]

        Parameters
        ----------
        rawcol, uniqcol: pymongo.collection.Collection
            collections from which to query the structure and from which to store
            unique _id
        e_threshold : float, optional
            enthalpy_per_atom threshold used to determine unique, by default 0.005
        match_kwargs: dict, optional
            other kwargs for pymatgen StructureMatcher
        """
        self.rawcol = rawcol
        self.uniqcol = uniqcol
        self.e_threshold = e_threshold
        self.matcher = StructureMatcher(**match_kwargs)

    @lru_cache
    def group(self, mindate=None, maxdate=None):
        """get the group records list newer than `newerdate`

        Parameters
        ----------
        newerdate : tuple
            (year, month, day, hour, minute, second, ...), fill from left

        Returns
        -------
        cursor: list
            record list of {"_id": <task, formula>, "count": <int>, "ids": [<_id>, ...]}
        """
        # [task_formula_group, ...]
        cursor = list(
            self.rawcol.aggregate(
                Pipes.daterange_records(mindate, maxdate) + Pipes.group_task_formula()
            )
        )
        return cursor

    def check(self, mindate=None, maxdate=None):
        cursor = self.group(mindate, maxdate)
        for cur in cursor:
            i_uniq_list = self.find_unique_in_group(cur)
            dup_one = self.uniqcol.find_one({"_id": {"$in": i_uniq_list}})
            if dup_one is not None:
                print(f"Found one duplicated in {self.uniqcol} : {dup_one}")
                print("Please try other newerdate")
            yield i_uniq_list

    def update(
        self, mindate=(1, 1, 1, 0, 0, 0), maxdate=(9999, 12, 31, 0, 0, 0), *, version
    ):
        cursor = self.group(mindate, maxdate)
        uniq_list = self.find_unique(cursor)
        try:
            self.uniqcol.insert_many(
                [{"_id": uniq_id, "version": version} for uniq_id in uniq_list],
                ordered=False,
            )
            print("Documents inserted successfully.")
        except pymongo.errors.DuplicateKeyError:
            print("Duplicate _id encountered. Skipped duplicate documents.")

    def find_unique(self, cursor):
        results = Parallel(backend="multiprocessing")(
            delayed(self.find_unique_in_group)(cur) for cur in tqdm(cursor)
        )
        uniq_list = list(chain.from_iterable(results))
        return uniq_list

    def find_unique_in_group(self, task_formula_group):
        """Find the unique structures' _id in task_formula group

        Parameters
        ----------
        task_formula_group : pymongo.command_cursor.CommandCursor
            a task_formula group yield by pipeline Pipes.group_task_formula

        Returns
        -------
        _type_
            _description_
        """
        ids = task_formula_group["ids"]
        # task = record_task_formula["_id"]["task"]
        # formula = record_task_formula["_id"]["formula"]
        # print(task, formula, record_task_formula["count"])
        projection = {"symmetry.1e-1.number": 1, "enthalpy_per_atom": 1}

        qs = QueryStructure(self.rawcol, projection)
        [qs.find({"_id": {"$in": ids}})]  # cache all structures in this group

        unique_list = []  # _id
        for i_id in ids:
            i_structure = qs[i_id]["_structure_"]
            i_properties = qs[i_id]
            for j_id in unique_list:
                j_structure = qs[j_id]["_structure_"]
                j_properties = qs[j_id]
                same_sym = (
                    i_properties["symmetry"]["1e-1"]["number"]
                    == j_properties["symmetry"]["1e-1"]["number"]
                )
                d_e = (
                    i_properties["enthalpy_per_atom"]
                    - j_properties["enthalpy_per_atom"]
                )
                # only compare with those with same symmetry and energy is close
                if same_sym and abs(d_e) < self.e_threshold:
                    match = self.matcher.fit(i_structure, j_structure)
                    # match, replace to the lowest energy one
                    if match:
                        if d_e < 0:
                            unique_list.remove(j_id)
                            unique_list.append(i_id)
                        break
                    # do not match, add
                    else:
                        unique_list.append(i_id)
                        # print(i_properties["enthalpy_per_atom"])
                        break
                # otherwise treat as a different one
            # do not match to any one, add
            else:
                unique_list.append(i_id)
        # result energy list
        # print(sorted([qs[_id][1]["enthalpy_per_atom"] for _id in unique_list]))
        return unique_list

    def maintain_deprecated(self):
        """delete the deprecated records in uniqcol"""
        pipeline = [{"$match": {"deprecated": True}}]
        pipeline += Pipes.unique_records(self.uniqcol.name)
        pipeline += [{"$group": {"_id": None, "ids": {"$push": "$_id"}}}]
        for record in self.rawcol.aggregate(pipeline):
            print(f"{len(record['ids'])} records will be deleted from {self.uniqcol}")
            self.uniqcol.delete_many({"_id": {"$in": record["ids"]}})
        print("Deleted")


if __name__ == '__main__':
    pass
    # ---------------------------------------------------------------
    # Uncomment the following to run find unique
    # db = login()
    # raw = db.get_collection("raw")
    # uniq = db.get_collection("uniq")
    # uniquefinder = UniqueFinder(raw, uniq)
    # newerdate = (2023, 6, 1)
    # uniquefinder.update(newerdate, version=20230616)
    # ---------------------------------------------------------------
