from pprint import pprint

from calypsokit.calydb.login import login


def test_connect(env: str, collection: str):
    db = login(dotenv_path=env)
    col = db.get_collection(collection)
    pprint(col.find_one())
