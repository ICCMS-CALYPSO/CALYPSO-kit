import click

from calypsokit.utils.lazy import LazyLoader

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


# @db.command()
# @click.option('--env', type=click.Path(), default='.env', help="env var, (.env)")
# @click.option('-c', '--collection', help="collection name")
# def clean_energy(env: str, collection: str):
#     assert isinstance(collection, str), "collection name must be a string"
#     pass
