"""Tests on an instantiated m3 app."""

import leip
import pytest
import pymongo


@pytest.fixture(scope='module')
def m3app():
    """Instantiate an m3 app."""
    yield leip.app(name='mad3')
    # print("teardown app")


@pytest.fixture(scope='module')
def testdb(m3app):
    """Return a test collection."""
    import mad3.db
    db = mad3.db.get_db(m3app)
    yield db.testdb
    db.drop_collection('testdb')


def test_m3app_init(m3app):
    """Does the app initialize."""
    assert isinstance(m3app, leip.app)


def test_m3app_loadconfig(m3app):
    """Does the app load configuration."""
    import fantail
    assert isinstance(m3app.conf, fantail.core.Fantail)
    assert 'keywords' in m3app.conf
    assert 'plugin' in m3app.conf
    assert 'dummydummydummy' not in m3app.conf
    assert 'core' in m3app.conf['plugin']


def test_m3app_hostname_is_set(m3app):
    """Is a hostname properly set."""
    assert 'hostname' in m3app.conf
    assert isinstance(m3app.conf['hostname'], str)


def test_m3app_dbworks(testdb):
    """Is there a proper db configured and can we load it."""
    import bson
    assert isinstance(testdb, pymongo.collection.Collection)
    result1 = testdb.insert_one(dict(a=1, b=2, c='3'))
    assert isinstance(result1, pymongo.results.InsertOneResult)
    postid = result1.inserted_id
    assert isinstance(postid, bson.objectid.ObjectId)

    newrec = testdb.find_one(dict(_id=postid))
    assert isinstance(newrec, dict)
    assert 'a' in newrec
    assert 'b' in newrec
    assert 'c' in newrec
    assert newrec['a'] == 1
    assert newrec['c'] == '3'
