import logging

import click

from calypsokit.utils.lazy import LazyLoader

logger = logging.getLogger(__name__)
funcs = LazyLoader('cli_db_funcs', globals(), 'calypsokit.commands.db_funcs')


@click.group("db")
def db():
    pass


@db.command()
@click.option('--env', type=click.Path(), default='.env', help="env var, (.env)")
@click.option('-c', '--collection', help="collection name")
def test_connect(env: str, collection: str):
    assert isinstance(collection, str), "collection name must be a string"
    funcs.test_connect(env, collection)


@db.command()
@click.option('--env', type=click.Path(), default='.env', help="env var, (.env)")
@click.option('-c', '--collection', help="collection name")
@click.option(
    '--mindate', nargs=6, default=None, help="year month day hour miniute second"
)
@click.option(
    '--maxdate', nargs=6, default=None, help="year month day hour miniute second"
)
def deprecate(env: str, collection: str, mindate: tuple, maxdate: tuple):
    assert isinstance(collection, str), "collection name must be a string"
    if mindate is not None:
        mindate = tuple(map(int, mindate))
    if maxdate is not None:
        maxdate = tuple(map(int, maxdate))
    funcs.deprecate(env, collection, mindate, maxdate)


@db.command()
@click.option('--env', type=click.Path(), default='.env', help="env var, (.env)")
@click.option('-c', '--collection', help="collection name")
def check_duplicate(env: str, collection: str):  # TODO: add switch to delete (--delete)
    assert isinstance(collection, str), "collection name must be a string"
    funcs.check_duplicate(env, collection)


@db.command()
@click.option('--env', type=click.Path(), default='.env', help="env var, (.env)")
@click.option('--rawcol', help="raw collection name")
@click.option('--uniqcol', help="unique collection name")
@click.option('--version', type=int, help="version number")
def find_unique(env: str, rawcol: str, uniqcol: str, version: int):
    funcs.find_unique(env, rawcol, uniqcol, version)
