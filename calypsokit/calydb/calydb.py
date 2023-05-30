import os

import dotenv
import pymongo


def connect_to_db(addr=None, user=None, pwd=None, db=None, col=None, dotenv_path=None):
    dotenv.load_dotenv(dotenv_path=dotenv_path, override=True)
    addr = os.environ.get('MONGO_ADDR', None) if addr is None else addr
    user = os.environ.get('MONGO_USER', None) if user is None else user
    pwd = os.environ.get('MONGO_PWD', None) if pwd is None else pwd
    db = os.environ.get('MONGO_DB', None) if db is None else db
    col = os.environ.get('MONGO_COLLECTION', None) if col is None else col
    for key in (addr, user, pwd, db, col):
        if key is None:
            raise ValueError(f"{key} not configured in .env file")

    client = pymongo.MongoClient(f"mongodb://{user}:{pwd}@{addr}")
    db = client[db]
    col = db[col]
    return db, col


# db, col = connect_to_db()
# print(col.find_one())
