"""Sub cli commands of analysis, the real funcs are lazy imported in funcs.py
"""
import click

from calypsokit.utils.lazy import LazyLoader

funcs = LazyLoader('cli_analy_funcs', globals(), 'calypsokit.cli_analy.funcs')


@click.group("analy")
def analy():
    pass


@analy.command()
@click.argument('root', type=click.Path())
@click.option('-L', '--level', type=int, default=1, help="recursive level (default 1)")
def list_results(root, level):
    """List all results* dir under <ROOT>"""
    funcs.find_results(root, level)


# @analy.command(help="check every given <results> dirs")
@analy.command()
@click.argument('root', type=click.Path())
@click.option(
    '-f', '--results_tree', type=click.Path(exists=True), help="file of results list"
)
def check_results(root, results_tree):
    """Check every given <results> dirs

    ROOT:   <ROOT>/(<date>/<name>/...)
    """
    funcs.check_basic_info(root, results_tree)


@analy.command()
@click.argument('root', type=click.Path())
@click.argument('results', type=click.Path())
def test_extract(root, results):
    """test extract ini-opt from one <results> dir

    ROOT:    <ROOT>/(<date>/<name>/)

    RESULTS: <path-to-results-dir>
    """
    funcs.extract_one(root, results)


@analy.command()
@click.argument('root', type=click.Path())
@click.option(
    '-f', '--results_tree', type=click.Path(exists=True), help="file of results list"
)
@click.option('-o', 'outpickle', type=click.Path())
def pickle_results(root, results_tree, outpickle):
    """extract and dump all ini-opt from given <results> dirs"""
    funcs.extract_all(root, results_tree, outpickle)
