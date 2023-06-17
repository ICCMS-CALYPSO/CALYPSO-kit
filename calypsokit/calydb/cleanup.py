from datetime import datetime

from calypsokit.calydb.queries import Pipes
from calypsokit.utils.itertools import groupby_delta


# 清理enthalpy=610612509的结构
def deprecate_large_enthalpy(collection):
    """mark those energy_per_atom > 610612508 as deprecated

    Will auto-check after run

    Examples
    --------
    >>> col: pymongo.collection.Collection
    >>> deprecate_large_enthalpy(col)

    Parameters
    ----------
    collection : collection
    """
    fil = {"deprecated": False, "enthalpy_per_atom": {"$gt": 610612508}}
    upd = {
        "$set": {
            "deprecated": True,
            "deprecated_reason": "error enthalpy : optimization fail",
        }
    }
    # Mark those as deprecated
    res = collection.update_many(fil, upd)
    print(f"{res.modified_count} records enthalpy/atom larger than 610612508 cleaned.")
    # Check if there exist left
    if collection.find_one(fil) is not None:
        print("Still exist enthalpy/atom larger than 610612508.")


# 清理每个任务少于lte(=10)个的结构（不考虑变组分）
def deprecate_less_task(collection, mindate=None, maxdate=None, lte: int = 10):
    """mark those number structures in one task <= `lte` as deprecated

    Will auto-check after run

    Examples
    --------
    >>> col: pymongo.collection.Collection
    >>> mindate = (2023, 1, 1)
    >>> lte = 10
    >>> deprecate_less_task(col, mindate, 10)

    Parameters
    ----------
    collection : pymongo.collection.Collection
    mindate, maxdate : tuple, optional
        utc date (year, month, day, hour, minute, second, ...) of 'last_updated_utc',
        None for 0 and 9999, all None for total.
    lte : int, optional
        <= threshold, by default 10
    """
    if mindate is None and maxdate is None:
        pipeline = Pipes.group_task(lte=lte) + [{"$match": {"deprecated": False}}]
    else:
        pipeline = (
            Pipes.daterange_records(mindate, maxdate)
            + Pipes.group_task(lte=lte)
            + [{"$match": {"deprecated": False}}]
        )
    udp = {
        "$set": {
            "deprecated": True,
            "deprecated_reason": f"number of extracted structure <= {lte}"
            + " in this prediction task",
        }
    }
    cursor = list(collection.aggregate(pipeline))
    # Mark those not deprecated as deprecated
    for record in cursor:
        collection.update_many(
            {"deprecated": False, "_id": {"$in": record["ids"]}}, udp
        )
    print(f"{len(cursor)} groups cleaned.")
    # Check if there exist left
    cleaned_flag = True
    for record in cursor:
        if (
            collection.find_one({"deprecated": False, "_id": {"$in": record["ids"]}})
            is not None
        ):
            print(f"Still exist uncleaned records: {record}")
            cleaned_flag = False
    if cleaned_flag:
        print(
            f"Cleaned task between ({mindate}, {maxdate}) "
            f"which number of structure <= {lte}"
        )


# 清理每组任务每个分子式中能量很低的孤立结构（间隔超过delta=1eV）的结构
def deprecate_solitary_enth(collection, mindate=None, maxdate=None, delta=1.0):
    """mark those solitary energy structure as deprecated

    Filter the newer (if do) and not deprecated records, group them by task and
    formula and sort group by enthalpy_per_atom (see Pipes.sort_enthalpy). Then
    find the solitary one accroding to `delta`, and mark it deprecated.

    Examples
    --------
    >>> col: pymongo.collection.Collection
    >>> clean_solitary_enth(col, (2023, 1, 1), delta=1.0)

    Parameters
    ----------
    collection : pymongo.collection.Collection
    mindate, maxdate : tuple, optional
        utc date (year, month, day, hour, minute, second, ...) of 'last_updated_utc',
        None for 0 and 9999, all None for total.
    delta : float, optional
        determine solitary by energy delta, by default 1.0
    """
    update_dict = {
        "deprecated": True,
        "deprecated_reason": "error enthalpy : solitary and too small",
    }
    if mindate is None and maxdate is None:
        pipeline = Pipes.sort_enthalpy()
    else:
        pipeline = Pipes.daterange_records(mindate, maxdate) + Pipes.sort_enthalpy()
    for record in collection.aggregate(pipeline):
        naccumu = 0
        # 只检查相邻能量间隔为1eV的前5组
        for ene_group in list(groupby_delta(record["sorted_enth"], delta))[:5]:
            naccumu += len(ene_group)
            if len(ene_group) >= 10:  # 若出现连续较长的组，则不再检查后面的组
                break
            elif len(ene_group) == 1:  # 孤立组，需要删除
                # solitary_id = record["sorted_ids"][naccumu - 1]
                print("Solitary enthalpy:", record["_id"])
                # yield (solitary_id, update_dict)
                collection.update_one(
                    {"_id": record["sorted_ids"][naccumu - 1]},
                    {"$set": update_dict},
                )


def deprecate_min_dist(collection):
    min_dist = 0.5
    update_dict = {
        "deprecated": True,
        "deprecated_reason": f"distance error : too close ({min_dist})",
    }
    collection.update_many(
        {"deprecated": False, "min_distance": {"$lt": min_dist}}, {"$set": update_dict}
    )


def clean_deprecated_unique(rawcol, uniqcol):
    """remove records marked deprecated in <rawcol> which still in <uniqcol>

    Examples
    --------
    >>> rawcol, uniqcol
    >>> clean_deprecated_unique(rawcol, uniqcol)

    Parameters
    ----------
    rawcol : pymongo.collection.Collection
    uniqcol : pymongo.collection.Collection
    """
    uniqcolname = uniqcol.name
    pipeline = [
        {"$match": {"deprecated": True}},
        {"$limit": 10},
        {"$project": {"_id": 1}},
        {
            "$lookup": {
                "from": uniqcolname,
                "localField": "_id",
                "foreignField": "_id",
                "as": "matched_id",
            }
        },  # {"_id": ..., "matched_id": [...]}
        {"$unwind": "$matched_id"},
    ]
    deprecated_in_uniq = [
        record["matched_id"]["_id"] for record in rawcol.aggregate(pipeline)
    ]
    if len(deprecated_in_uniq) == 0:
        print("Nothing to clean")
    else:
        print(
            f"{len(deprecated_in_uniq)} deprecated records will be removed "
            f"from col: {uniqcol.name}"
        )
        uniqcol.delete_many({"_id": {"$in": deprecated_in_uniq}})
