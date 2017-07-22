

from functools import lru_cache

import pymongo

CLIENT = None
DB = None


def get_db(app):
    global CLIENT, DB
    if CLIENT is None:
        dbinf = app.conf['db']
        CLIENT = pymongo.MongoClient(dbinf['host'])

    if DB is None:
        DB = getattr(CLIENT, dbinf['db'])
        DB.authenticate(dbinf['user'], app.conf['db']['pass'], mechanism='SCRAM-SHA-1')
    return DB
