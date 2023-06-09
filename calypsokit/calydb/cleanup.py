from datetime import datetime

from calypsokit.calydb.queries import Pipes
from calypsokit.utils.itertools import groupby_delta


# 清理enthalpy=610612509的结构
def deprecate_large_enthalpy(collection):
    fil = {"deprecated": False, "enthalpy_per_atom": {"$gt": 610612508}}
    upd = {
        "$set": {
            "deprecated": True,
            "deprecated_reason": "error enthalpy : optimization fail",
        }
    }
    # Mark those as deprecated
    collection.update_many(fil, upd)
    # Check if there exist left
    if collection.find_one(fil) is not None:
        print("Still exist energy_per_atom larger than 610612508")
    else:
        print("Cleaned energy_per_atom larger than 610612508")


# 清理每个任务少于lte(=10)个的结构（不考虑变组分）
def deprecate_less_task(collection, newerdate, lte: int = 10):
    pipeline = Pipes.newer_records(*newerdate) + Pipes.group_task(lte=lte)
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
            {"deprecated": "False", "_id": {"$in": record["ids"]}}, udp
        )
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
            f"Cleaned task newer than ({datetime(*newerdate)}) "
            f"which number of structure <= {lte}"
        )


# 清理每组任务每个分子式中能量很低的孤立结构（间隔超过delta=1eV）的结构
def clean_solitary_enth(col, delta=1.0):
    update_dict = {
        "deprecated": True,
        "deprecated_reason": "error enthalpy : solitary and too small",
    }
    pipeline = Pipes.sort_enthalpy()
    for record in col.aggregate(pipeline):
        naccumu = 0
        # 只检查相邻能量间隔为1eV的前5组
        for ene_group in list(groupby_delta(record["sorted_enth"], delta))[:5]:
            naccumu += len(ene_group)
            if len(ene_group) >= 10:  # 若出现连续较长的组，则不再检查后面的组
                break
            elif len(ene_group) == 1:  # 孤立组，需要删除
                solitary_id = record["sorted_ids"][naccumu - 1]
                yield (solitary_id, update_dict)
                # col.update_one(
                #     {"_id": record["sorted_ids"][naccumu - 1]},
                #     {"$set": update_dict},
                # )
