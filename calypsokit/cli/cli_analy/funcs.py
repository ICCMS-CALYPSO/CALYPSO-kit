import pickle

from pathlib import Path
from pprint import pprint

import click

from calypsokit.analysis.legacy.extract_iniopt import (
    GroupIniOpt,
    get_results_dir,
    patch_before_insert,
)


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
