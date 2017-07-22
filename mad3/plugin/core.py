"""
Core functions for Mad3
"""

import leip
import pymongo

from mad3.db import get_db
from mad3.madfile import MadFile


@leip.command
def create_index(app, args):
    """Create mongodb indici."""
    db = get_db(app)
    for idx in app.conf['index']['transient']:
        db.transient.create_index([(idx, pymongo.ASCENDING)])


@leip.arg('file')
@leip.command
def show(app, args):
    "Show file metadata"

    mf = MadFile(app, args.file)
    for k in mf.keys():
        print("{}\t{}".format(k, mf[k]))


@leip.arg('file')
@leip.arg('value')
@leip.arg('key')
@leip.command
def set(app, args):
    """Show file metadata"""
    mf = MadFile(app, args.file)
    mf[args.key] = args.value
