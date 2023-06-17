import click

from calypsokit import log
from calypsokit.commands.analy_cli import analy
from calypsokit.commands.db_cli import db


@click.group()
@click.option('-v', '--verbose', count=True)
def cak(verbose):
    level = 20 - verbose * 3
    log.init(level)


def main():
    cak.add_command(analy)
    cak.add_command(db)
    cak()
