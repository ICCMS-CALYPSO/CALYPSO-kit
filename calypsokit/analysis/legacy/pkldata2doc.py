import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
from joblib import Parallel, delayed
from tqdm import tqdm

from calypsokit.calydb.login import login
from calypsokit.calydb.record import RecordDict


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


def legacydata_to_record_one(calyidx, data_file):
    db, col = login(col="rawcol")
    try:
        data = pickle.load(open(data_file, "rb"))
        # ------------------------------------------------------------------------
        data["material_id"] = f"caly-{calyidx}"
        # ------------------------------------------------------------------------
        data["source"] = {"name": "calypso", "index": calyidx}
        # ------------------------------------------------------------------------
        source_dir = str(Path(data["trajectory"]["source"][0]).parent)
        data["trajectory"].update({"soruce_dir": source_dir})
        # ------------------------------------------------------------------------
        if data["nelements"] != data["calyconfig"]["numberofspecies"]:
            return False
        # ------------------------------------------------------------------------
        if data["pseudopotential"][0] == "NotFound":
            data["pseudopotential"] = [None] * data["nelements"]
        data["pseudopotential"] = data["pseudopotential"][: data["nelements"]]
        # ------------------------------------------------------------------------
        if ("layertype" in data["calyconfig"]) and (
            data["calyconfig"]["layertype"] is not None
        ):
            data["calyconfig"]["layertype"] = [
                {u: int(v) for u, v in layer.items()}
                for layer in data["calyconfig"]["layertype"]
            ]
        elif ("ctrlrange" in data["calyconfig"]) and (
            data["calyconfig"]["ctrlrange"] is not None
        ):
            data["calyconfig"]["ctrlrange"] = {
                u: [int(i) for i in v]
                for u, v in data["calyconfig"]["ctrlrange"].items()
            }
        # ------------------------------------------------------------------------
        record = RecordDict(data)
    except Exception as e:
        print(data_file, e)
    else:
        # print(record)
        record["calyconfig"].pop("ctrlrange")
        col.insert_one(record)
    # return record


def legacydata_to_record(pkl_list, currentmaxcalyidx):
    # create entirely new
    record_list = Parallel(backend="multiprocessing")(
        delayed(legacydata_to_record_one)(int(Path(data_file).stem), data_file)
        for data_file in tqdm(pkl_list, total=len(pkl_list))
    )
    # add to exist col
    # record_list = Parallel(backend="multiprocessing")(
    #     delayed(legacydata_to_record_one)(calyidx, data_file)
    #     for calyidx, data_file in tqdm(
    #         enumerate(pkl_list, currentmaxcalyidx + 1), total=len(pkl_list)
    #     )
    # )
    return record_list


def wrapper_legacydata_to_record(pkl_folder):
    db, col = login(col="rawcol")
    currentmaxcalyidx = get_current_caly_max_index(col)
    print(currentmaxcalyidx)
    pkl_list = [str(p) for p in Path(pkl_folder).glob("*.pkl")]
    record_list = legacydata_to_record(pkl_list, currentmaxcalyidx)
    return record_list


def wrapper_legacydata_to_record_rest(insert_log):
    db, col = login(col="rawcol")
    currentmaxcalyidx = get_current_caly_max_index(col)
    print(currentmaxcalyidx)
    with open(insert_log, "r") as f:
        pkl_list = [li.split()[0] for li in f.readlines() if "cache" in li]
    record_list = legacydata_to_record(pkl_list, currentmaxcalyidx)
    return record_list


def update_timestamp(collection):
    collection.update_many({}, {"$set": {"last_updated_utc": datetime.utcnow()}})


if __name__ == "__main__":
    db, col = login(col="rawcol")
    # --- insert to db
    wrapper_legacydata_to_record("cache")
    # --- fixed and insert the rest
    # wrapper_legacydata_to_record_rest("insert.log")
    # --- update timestamp
    # update_timestamp(col)
