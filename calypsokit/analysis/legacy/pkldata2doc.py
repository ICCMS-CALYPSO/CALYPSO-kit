import pickle
from pathlib import Path

import numpy as np
from calypsokit.calydb.login import login
from calypsokit.calydb.record import RecordDict
from joblib import Parallel, delayed
from tqdm import tqdm


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


db, col = login(col="rawcol")


def legacydata_to_record_one(calyidx, data_file):
    # db, col = login(col="debugcol")
    # col.insert_one(RecordDict())
    try:
        data = pickle.load(open(data_file, "rb"))
        data["material_id"] = f"caly-{calyidx}"
        data["source"] = {"name": "calypso", "index": calyidx}
        if data["nelements"] != data["calyconfig"]["numberofspecies"]:
            return False
        if data["pseudopotential"][0] == "NotFound":
            data["pseudopotential"] = [None] * data["nelements"]
        data["pseudopotential"] = data["pseudopotential"][: data["nelements"]]
        if "layertype" in data["calyconfig"]:
            data["calyconfig"]["layertype"] = [
                {u: int(v) for u, v in layer.items()}
                for layer in data["calyconfig"]["layertype"]
            ]
        elif "ctrlrange" in data["calyconfig"]:
            data["calyconfig"]["ctrlrange"] = {
                u: [int(i) for i in v]
                for u, v in data["calyconfig"]["ctrlrange"].items()
            }
        record = RecordDict(data)
    except Exception as e:
        print(data_file, e)
    else:
        # print(record)
        record["calyconfig"].pop("ctrlrange")
        col.insert_one(record)
    # return record


# {
#     'version': 'v.5.0',
#     'systemname': 'LiP',
#     'numberofspecies': 2,
#     'nameofatoms': ['Li', 'P'],
#     'numberofatoms': [5, 1],
#     'numberofformula': [2, 2],
#     'volume': [0.0],
#     'distanceofion': array([[0.8, 0.8], [0.8, 0.8]]),
#     'ialgo': 2,
#     'icode': 1,
#     'numberoflocaloptim': 3,
#     'psoratio': 0.6,
#     'popsize': 30,
#     'kgrid': (0.12, 0.06),
#     'command': 'sh submit.sh',
#     'maxstep': 50,
#     'pickup': False,
#     'pickupstep': None,
#     'maxtime': 7200.0,
#     'lmc': False,
#     '2d': False,
#     'lfilm': False,
#     'thickness': None,
#     'area': 10.0,
#     'multilayer': 3,
#     'deltaz': 0.0,
#     'layergap': 2.0,
#     'vacuumgap': 40.0,
#     'layertype': [{'Li': 1, 'P': 1}, {'Li': 0, 'P': 0}, {'Li': 0, 'P': 0}],
#     'latom_dis': 1.4,
#     'cluster': False,
#     'vacancy': (10.0, 10.0, 10.0),
#     'mol': False,
#     'numberoftypemolecule': None,
#     'numberofmolecule': None,
#     'distofmol': 1.5,
#     'vsc': False,
#     'maxnumatom': 20,
#     'ctrlrange': None,
#     'lsurface': False,
#     'surfacethickness': 3.0,
#     'substrate': 'SUBSTRATE.surf',
#     'surface_atoms': None,
#     'spacesaving': True,
#     'ecr': False,
#     'ciffilepath': None,
#     'millerindex': None,
#     'matrixnotation': None,
#     'slabvaccumthick': 10,
#     'slabtopmost': ['CALYPSO'],
#     'slabnumlayers': 6,
#     'numrelaxedlayers': 2,
#     'capbondswithh': True,
#     'linterface': False,
#     'interfacetranslation': False,
#     'translationlimita': 0.0,
#     'translationlimitb': 0.0,
#     'rand_scheme': 1,
#     'interface_thickness': None,
#     'forbi_thickness': 0.2,
#     'interface_atoms': None,
#     'coordinate_number': None,
#     'substrate2': 'SUBSTRATE2.surf',
#     'twin_interface': 0,
# }


def legacydata_to_record(pkl_list, currentmaxcalyidx):
    record_list = Parallel(backend="multiprocessing")(
        delayed(legacydata_to_record_one)(calyidx, data_file)
        for calyidx, data_file in tqdm(
            enumerate(pkl_list, currentmaxcalyidx + 1), total=len(pkl_list)
        )
    )
    return record_list


def wrapper_legacydata_to_record(pkl_folder):
    currentmaxcalyidx = get_current_caly_max_index(col)
    print(currentmaxcalyidx)
    pkl_list = [str(p) for p in Path(pkl_folder).glob("*.pkl")]
    record_list = legacydata_to_record(pkl_list, currentmaxcalyidx)
    # col.insert_many([record for record in record_list if record])


if __name__ == "__main__":
    wrapper_legacydata_to_record("cache")
