import click
from pprint import pprint


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


@analy.command('extract_one', help="extract grouped ini-opt from results*")
@click.argument('root', type=click.Path())
@click.argument('results', type=click.Path())
def extract_one(root, results):
    from calypsokit.analysis.legacy.extract_iniopt import GroupIniOpt

    for datadict in GroupIniOpt(root).group_one_results(results):
        pprint(datadict)
        break


def main():
    cak.add_command(analy)
    cak()
