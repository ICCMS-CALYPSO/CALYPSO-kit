import click

from calypsokit.commands.analy_cli import analy
from calypsokit.commands.db_cli import db


@click.group()
def cak():
    pass


def main():
    cak.add_command(analy)
    cak.add_command(db)
    cak()
