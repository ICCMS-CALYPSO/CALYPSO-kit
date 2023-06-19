import logging

import pickle
from pathlib import Path
from pprint import pprint

import click
from joblib import Parallel, delayed
from tqdm import tqdm

from calypsokit.analysis.legacy.extract_iniopt import (
    GroupIniOpt,
    get_results_dir,
    patch_before_insert,
    wrapper_patch_before_insert,
)
from calypsokit.calydb.login import login
from calypsokit.utils.itertools import batched

logger = logging.getLogger(__name__)


def find_results(root, level):
    for result_dir in get_results_dir(root, level):
        click.echo(result_dir)


def check_basic_info(root, results_tree):
    groupiniopt = GroupIniOpt.from_file(root, results_tree)
    list(groupiniopt.check_basic_info())


def extract_one(root, results):
    for datadict in GroupIniOpt(root).group_one_results(results):
        pprint(patch_before_insert(0, datadict))
        break


def extract_all(root, results_tree, outpickle):
    datadict_list = GroupIniOpt(root).from_file(root, results_tree)()
    pickle_file = Path(results_tree).with_name(outpickle)
    with open(pickle_file, 'wb') as f:
        pickle.dump(datadict_list, f)


def insert_results(root, results_tree, config, collection):
    db = login(dotenv_path=config)
    col = db.get_collection(collection)

    with open(results_tree, 'r') as f:
        results_list = [line.strip() for line in f.readlines()]

    print(f"Total results dir: {len(results_list)}")
    for iresults, results_dir in enumerate(results_list):
        print(f"{iresults=}")
        datadict_list = GroupIniOpt(root, [results_dir])()
        rawrecord_list = Parallel(30, backend="multiprocessing")(
            delayed(wrapper_patch_before_insert)(calyidx, datadict)
            for calyidx, datadict in tqdm(enumerate(datadict_list, 0))
        )
        rawrecord_list = [rec for rec in rawrecord_list if rec is not None]
        if len(rawrecord_list) > 0:
            print(f"In iresults {iresults} , inserting {len(rawrecord_list)}")
            col.insert_many(rawrecord_list)
        else:
            print(f"{results_dir} has no valid record")
