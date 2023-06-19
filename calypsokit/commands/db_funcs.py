import logging
from pprint import pprint

import calypsokit.calydb.cleanup as cleanup
import calypsokit.calydb.queries as queries
from calypsokit.analysis.find_unique import UniqueFinder
from calypsokit.calydb.login import login
from calypsokit.calydb.readout import ReadOut


def test_connect(env: str, collection: str):
    db = login(dotenv_path=env)
    col = db.get_collection(collection)
    pprint(col.find_one())


def deprecate(env: str, collection: str, mindate, maxdate):
    db = login(dotenv_path=env)
    col = db.get_collection(collection)
    cleanup.deprecate_large_enthalpy(col)
    cleanup.deprecate_less_task(col, mindate, maxdate)
    cleanup.deprecate_solitary_enth(col, mindate, maxdate)
    cleanup.deprecate_min_dist(col)


def check_duplicate(env: str, collection: str):
    db = login(dotenv_path=env)
    col = db.get_collection(collection)
    queries.check_duplicate(col)


def find_unique(env: str, rawcol: str, uniqcol: str, version):
    db = login(dotenv_path=env)
    rawcol = db.get_collection(rawcol)
    uniqcol = db.get_collection(uniqcol)
    version = int(version)
    uniquefinder = UniqueFinder(rawcol, uniqcol)
    uniquefinder.update(version=version)


def maintain_unique(env: str, rawcol: str, uniqcol: str):
    db = login(dotenv_path=env)
    rawcol = db.get_collection(rawcol)
    uniqcol = db.get_collection(uniqcol)
    uniquefinder = UniqueFinder(rawcol, uniqcol)
    uniquefinder.maintain_deprecated()


def readout(
    env: str = None,
    rawcol: str = None,
    uniqcol: str = None,
    type: str = None,
    outfile: str = None
):
    db = login(dotenv_path=env)
    if type == 'cdvae':
        df = ReadOut().unique2cdvae(db, rawcol, uniqcol)
        df.to_feather(outfile)
