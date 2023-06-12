import os
import pickle

import dotenv
import numpy as np
import pymongo
from bson import Binary
from bson.binary import USER_DEFINED_SUBTYPE
from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry
from pymongo.database import Database


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


class NumpyDatabase(Database):
    numpy_codec = NumpyCodec()
    type_registry = TypeRegistry([numpy_codec], fallback_encoder=fallback_encoder)
    codec_options: CodecOptions
    codec_options = CodecOptions(type_registry=type_registry, tz_aware=False)

    def __init__(self, client, name, codec_options=codec_options, **kwargs):
        super().__init__(client, name, codec_options, **kwargs)


def login(addr=None, user=None, pwd=None, db=None, dotenv_path=None):
    dotenv.load_dotenv(dotenv_path=dotenv_path, override=True)
    addr = os.environ.get('MONGODB_ADDR', None) if addr is None else addr
    user = os.environ.get('MONGODB_USER', None) if user is None else user
    pwd = os.environ.get('MONGODB_PWD', None) if pwd is None else pwd
    dbname = os.environ.get('MONGODB_DATABASE', None) if db is None else db

    for key in (addr, user, pwd, dbname):
        if key is None:
            raise ValueError(f"{key} not configured in .env file")

    client = pymongo.MongoClient(f"mongodb://{user}:{pwd}@{addr}")
    db = NumpyDatabase(client, dbname)
    return db


def maintain_indexes(col) -> dict:
    iinfo = col.index_information()
    if ("material_id_1" in iinfo) and (not iinfo["material_id_1"].get("unique", False)):
        col.drop_index("material_id_1")

    iinfo = col.index_information()
    if "material_id_1" not in iinfo:
        col.create_index([("material_id", 1)], unique=True, name="material_id_1")
    elif "deprecated_1" not in iinfo:
        col.create_index([("deprecated", 1)], name="deprecated_1")

    return col.index_information()
