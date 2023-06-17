from pprint import pprint

import calypsokit.calydb.cleanup as cleanup
import calypsokit.calydb.queries as queries
from calypsokit.calydb.login import login


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


def check_duplicate(env: str, collection: str):
    db = login(dotenv_path=env)
    col = db.get_collection(collection)
    queries.check_duplicate(col)
