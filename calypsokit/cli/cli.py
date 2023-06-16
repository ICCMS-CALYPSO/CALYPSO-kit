import click

from calypsokit.cli.cli_analy.cli import analy


@click.group()
def cak():
    pass


def main():
    cak.add_command(analy)
    cak()
