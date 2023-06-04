import os
import pickle

import dotenv
import numpy as np
import pymongo
from bson import Binary
from bson.binary import USER_DEFINED_SUBTYPE
from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry


# =========== Registor Numpy Type ==========
class NumpyCodec(TypeCodec):
    python_type = np.ndarray
    bson_type = Binary

    def transform_python(self, value):
        return Binary(pickle.dumps(value, protocol=2), USER_DEFINED_SUBTYPE)

    def transform_bson(self, value):
        if value.subtype == USER_DEFINED_SUBTYPE:
            return pickle.loads(value)
        return value


def fallback_encoder(value):
    if isinstance(value, np.ndarray):
        return Binary(pickle.dumps(value, protocol=2), USER_DEFINED_SUBTYPE)
    return value


def get_codec_options():
    numpy_codec = NumpyCodec()
    type_registry = TypeRegistry([numpy_codec], fallback_encoder=fallback_encoder)
    codec_options = CodecOptions(type_registry=type_registry, tz_aware=False)
    return codec_options


def get_collection(name, db):
    codec_options = get_codec_options()
    return db.get_collection(name, codec_options=codec_options)


def login(addr=None, user=None, pwd=None, db=None, col=None, dotenv_path=None):
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
    col = get_collection(col, db)

    all_index = col.index_information()
    if all_index.get("material_id", False):
        col.ensure_index([("material_id", 1)], unique=True)
    else:
        col.create_index([("material_id", 1)], unique=True)

    return db, col