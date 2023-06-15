import pickle
from pprint import pprint

import click


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
    from calypsokit.analysis.legacy.extract_iniopt import get_results_dir

    for result_dir in get_results_dir(root, level):
        click.echo(result_dir)


@analy.command('check_basic_info', help="check every results*")
@click.argument('root', type=click.Path())
@click.argument('results_tree', type=click.Path(exists=True))
def check_basic_info(root, results_tree):
    from calypsokit.analysis.legacy.extract_iniopt import GroupIniOpt

    groupiniopt = GroupIniOpt.from_file(root, results_tree)
    list(groupiniopt.check_basic_info())


@analy.command('extract_one', help="extract grouped ini-opt from results*")
@click.argument('root', type=click.Path())
@click.argument('results', type=click.Path())
def extract_one(root, results):
    from calypsokit.analysis.legacy.extract_iniopt import (
        GroupIniOpt,
        patch_before_insert,
    )

    for datadict in GroupIniOpt(root).group_one_results(results):
        pprint(patch_before_insert(0, datadict))
        break


@analy.command('extract_all', help="extract all grouped ini-opt from results*")
@click.argument('root', type=click.Path())
@click.argument('results_tree', type=click.Path(exists=True))
@click.option('-o', '--output', type=click.Path(), help="output pickle")
def extract_all(root, results_tree, output):
    pass


def main():
    cak.add_command(analy)
    cak()
