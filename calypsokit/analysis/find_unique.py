# Find the unique structure in calypso data
# DO NOT run this file directly unless you know what you are doing!
# This file may need modification for more specific unique-finding method


from datetime import datetime
from itertools import chain

from joblib import Parallel, delayed
from pymatgen.analysis.structure_matcher import StructureMatcher
from tqdm import tqdm

from calypsokit.calydb.login import login
from calypsokit.calydb.queries import QueryStructure, pipeline_group_task_formula


# 1. Group structures by task and formula.
# 2. For each structure in on group, one is compared to every picked structures.
# 3. If this two structures have the same space group (1e-1) and energy differs less
#    than 5meV, then they need to *match*. Otherwise they are considered to be
#    different and pick the one directly.
# 4. If matched, they are the same, the lower energy one will replace higher one.
# 5. If not matched, this one is added to the picked structures.
# 6. Finally the picked structures are the uniqe structures.
def find_unique(record_task_formula, col="rawcol", e_threshold=0.005, **match_kwargs):
    db, col = login(col=col)
    # task = record_task_formula["_id"]["task"]
    # formula = record_task_formula["_id"]["formula"]
    # print(task, formula, record_task_formula["count"])
    matcher = StructureMatcher(**match_kwargs)
    projection = {"symmetry.1e-1.number": 1, "enthalpy_per_atom": 1}
    qs = QueryStructure(col, projection)
    ids = record_task_formula["ids"]
    [qs.find({"_id": {"$in": ids}})]
    unique_list = []  # _id
    for i_id in ids:
        i_structure, i_properties = qs[i_id]
        for j_id in unique_list:
            j_structure, j_properties = qs[j_id]
            same_sym = (
                i_properties["symmetry"]["1e-1"]["number"]
                == j_properties["symmetry"]["1e-1"]["number"]
            )
            d_e = i_properties["enthalpy_per_atom"] - j_properties["enthalpy_per_atom"]
            # only compare with those with same symmetry and energy is close
            if same_sym and abs(d_e) < e_threshold:
                match = matcher.fit(i_structure, j_structure)
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


if __name__ == '__main__':
    # ---------------------------------------------------------------
    # Uncomment the following to run find unique
    # pipeline = pipeline_group_task_formula()
    # # pipline.append({"$match": {"count": {"$lt": 20}}})
    # # pipline.append({"$limit": 3})
    # db, col = login(col="rawcol")
    # cur = list(col.aggregate(pipeline))
    # results = Parallel(backend="multiprocessing")(
    #     delayed(find_unique)(record_task_formula) for record_task_formula in tqdm(cur)
    # )
    # unique_list = list(chain.from_iterable(results))
    # ---------------------------------------------------------------

    import pickle

    # with open("unique.pkl", "rb") as f:
    #     unique_list = pickle.load(f)
    # # print(unique_list)
    # db, uniqcol = login(col="uniqcol", dotenv_path=".env-maintain")
    # uniqcol.insert_many(
    #     [{"_id": uniq_id, "version": 20230601} for uniq_id in unique_list]
    # )

    # data = {"unique_ids": unique_list, "last_updated_utc": datetime.utcnow()}
    # uniqcol.insert_one(data)
