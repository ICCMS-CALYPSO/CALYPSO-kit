import pickle
from pathlib import Path
from pprint import pprint

import click

from calypsokit.analysis.legacy.extract_iniopt import (
    GroupIniOpt,
    get_results_dir,
    patch_before_insert,
)


@click.group()
def cak():
    pass


@click.group("analy")
def analy():
    pass


@analy.command('find_results', help="find dir named results*")
@click.argument('root', type=click.Path())
@click.option('-L', '--level', type=int, default=1, help="recursive level (default 1)")
def find_results(root, level):
    for result_dir in get_results_dir(root, level):
        click.echo(result_dir)


@analy.command('check_basic_info', help="check every results*")
@click.argument('root', type=click.Path())
@click.argument('results_tree', type=click.Path(exists=True))
def check_basic_info(root, results_tree):
    groupiniopt = GroupIniOpt.from_file(root, results_tree)
    list(groupiniopt.check_basic_info())


@analy.command('extract_one', help="extract grouped ini-opt from results*")
@click.argument('root', type=click.Path())
@click.argument('results', type=click.Path())
def extract_one(root, results):
    for datadict in GroupIniOpt(root).group_one_results(results):
        pprint(patch_before_insert(0, datadict))
        break


@analy.command('extract_all', help="extract all grouped ini-opt from results*")
@click.argument('root', type=click.Path())
@click.argument('results_tree', type=click.Path(exists=True))
@click.argument('outpickle', type=click.Path())
def extract_all(root, results_tree, outpickle):
    datadict_list = GroupIniOpt(root).from_file(root, results_tree)()
    pickle_file = Path(results_tree).with_name(outpickle)
    with open(pickle_file, 'wb') as f:
        pickle.dump(datadict_list, f)


def main():
    cak.add_command(analy)
    cak()
