from calypsokit.calydb.queries import pipe_group_task, pipe_sort_enthalpy
from calypsokit.utils.itertools import groupby_delta


# 清理enthalpy=610612509的结构
def pipe_deprecate_large_enthalpy():
    pipeline = [
        {"$match": {"deprecated": False, "enthalpy_per_atom": {"$gt": 610612508}}},
        {
            "$set": {
                "deprecated": True,
                "deprecated_reason": "error enthalpy : optimization fail",
            }
        },
    ]
    return pipeline


# 清理每个任务少于lte(=10)个的结构（不考虑变组分）
def pipe_deprecate_less_task(lte: int = 10):
    pipeline = pipe_group_task(lte) + [
        # {
        #     "$set": {
        #         "deprecated": True,
        #         "deprecated_reason": f"number of extracted structure <= {lte}"
        #         + " in this prediction task",
        #     }
        # },
        # TODO: ERROR
    ]
    return pipeline


# 清理每组任务每个分子式中能量很低的孤立结构（间隔超过delta=1eV）的结构
def clean_solitary_enth(col, delta=1.0):
    update_dict = {
        "deprecated": True,
        "deprecated_reason": "error enthalpy : solitary and too small",
    }
    pipeline = pipe_sort_enthalpy()
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
