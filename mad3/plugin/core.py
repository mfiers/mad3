"""
Core functions for Mad3
"""

import logging
import colors

import leip
import pymongo

from mad3.db import get_db
from mad3.madfile import MadFile
from mad3.util import key_info

lg = logging.getLogger(__name__)


#@leip.flag('-T', '--transaction', help='drop transaction db')
@leip.flag('--i_know_what_im_doing', help='Really do this? Note: DANGEROUS!!')
@leip.flag('--core', help='drop core db')
@leip.flag('--transient', help='drop transient db')
@leip.flag('--transaction', help='drop transaction db')
@leip.command
def drop(app, args):
    """Drop all data from one ore more databases"""
    if not args.i_know_what_im_doing:
        app.warning("Really drop? Add another command line flag")
    db = get_db(app)
    if args.transient:
        db.transient.drop()
    if args.core:
        db.core.drop()
    if args.transaction:
        db.transaction.drop()


@leip.command
def create_index(app, args):
    """Create mongodb indici."""
    db = get_db(app)
    for idx in app.conf['index']['transient']:
        db.transient.create_index([(idx, pymongo.ASCENDING)])
    for idx in app.conf['index']['transaction']:
        db.transaction.create_index([(idx, pymongo.ASCENDING)])


@leip.arg('term', nargs='*')
@leip.command
def find(app, args):

    query = {}

    db = get_db(app)

    for term in args.term:
        rawkey, rawval = term.split('=', 1)
        keyname, keyinfo = key_info(app, rawkey)
        val = keyinfo['transformer'](rawval)
        lg.info('search: {} {}'.format(keyname, val))
        if keyinfo['shape'] == 'one':
            if keyname not in query:
                query[keyname] = {"$in": [val]}
            else:
                raise NotImplemented()
        elif keyinfo['shape'] == 'set':
            if keyname not in query:
                query[keyname] = {'$all': [val]}

    for rec in db.transient.find(query):
        print(rec['filename'])


@leip.flag('-H', '--human', help='human readable')
@leip.arg('file')
@leip.command
def show(app, args):
    """Show file metadata."""
    mf = MadFile(app, args.file)
    if not args.human:
        for k in mf.keys():
            print('{}\t{}'.format(k, mf[k]))
    else:
        keylen = max([len(k) for k in mf.keys()])
        mfs = '{:' + str(keylen) + '}'
        for k in sorted(mf.keys()):
            if k != '_id':
                kname, kinfo = key_info(app.conf, k)
                tag = ''
                for cc in 'transient core'.split():
                    if cc in kinfo['cat']:
                        tag += colors.color(cc[0],
                                            fg=app.conf['color'][cc]['fg'],
                                            bg=app.conf['color'][cc]['bg'])
                    else:
                        tag += ' '
                val = mf[k] if kinfo['shape'] == 'one' else '|'.join(mf[k])
                print(tag,
                      colors.color(mfs.format(k)),
                      colors.color(': {}'.format(val)), sep=' ')


@leip.arg('file')
@leip.command
def save(app, args):
    """Ensure all metadata is saved."""
    mf = MadFile(app, args.file)
    mf.save()


@leip.arg('file')
@leip.arg('value')
@leip.arg('key')
@leip.command
def set(app, args):
    """Show file metadata"""
    mf = MadFile(app, args.file)
    mf[args.key] = args.value


@leip.arg('value', nargs='?')
@leip.arg('key')
@leip.command
def forget(app, args):
    """Forget a key, or key value combination."""
    key, kinfo = key_info(app.conf, args.key)

    db = get_db(app)
    if args.value:
        lg.warning("forget key=value {}={}".format(key, args.value))
        if kinfo.get('shape', 'one') == 'set':
            db.transient.update({}, {"$pull": {key: args.value}},
                                upsert=False, multi=True)
            db.core.update({}, {"$pull": {key: args.value}},
                           upsert=False, multi=True)
        else:
            raise NotImplemented()
    else:
        lg.warning("forget key {}".format(key))
        raise NotImplemented()
