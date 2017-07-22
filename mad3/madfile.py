

from datetime import datetime
import hashlib
import logging
import os
import stat

import pymongo.errors

from mad3.db import get_db
from mad3.util import key_info

lg = logging.getLogger(__name__)


def bulk_init(app):
    lg.debug("start bulk mode")
    db = get_db(app)
    app.bulk_mode = True
    app.bulk_transient = db.transient.initialize_unordered_bulk_op()
    app.bulk_core = db.core.initialize_unordered_bulk_op()


def bulk_execute(app):
    lg.debug("Executing bulk operations")
    try:
        app.bulk_transient.execute()
    except pymongo.errors.InvalidOperation as e:
        if e.args[0] == 'No operations to execute':
            lg.info("no bulk data to store to transient")
        else:
            raise
    except pymongo.errors.BulkWriteError as e:
        #from pprint import pprint
        #pprint(e.details)
        raise 

    try:
        app.bulk_core.execute()
    except pymongo.errors.InvalidOperation as e:
        if e.args[0] == 'No operations to execute':
            lg.info("no bulk data to store to core")
        else:
            raise


def setone(data, k, v):
    """Simply set a value, and check if it has changed."""
    if (k in data) and (data[k] == v):
        return False, v
    data[k] = v
    return True, v


def setset(data, k, v):
    """Add a value to a set, and check if it has changed."""

    if k in data:
        if not isinstance(data[k], list):
            data[k] = [data[k]]
        if v in data[k]:
            return False, data[k]
        else:
            data[k].append(v)
            return True, data[k]
    else:
        data[k] = [v]
        return True, data[k]


class MadFile:
    """Representing a file + metadata."""

    def __init__(self, app, filename):
        """Prepare the MadFile."""
        self.app = app
        self.dirty = False
        
        self.app.counter['init_madfile'] += 1

        filename = os.path.abspath(os.path.expanduser(filename))
        assert os.path.exists(filename)

        self.filename = filename
        self.filestat = os.stat(filename)

        # step one - determine transient id of this file
        lg.debug('calc transient id for {}'.format(self.filename))
        self.transient_id = self.get_transient_id()
        lg.debug(' -- transient id is {}'.format(self.transient_id))

        # check the database if a record with the transient id exists
        self.db = get_db(app)
        lg.debug('check transient rec for {}'.format(self.filename))
        self.transient_rec = self.db.transient.find_one({'_id': self.transient_id})

        # if there is no transient rec, calculate core id
        if self.transient_rec is None:
            self.app.counter['notrans'] += 1

            lg.debug('transient record does not exist')

            # needs to be saved now
            self.dirty = True

            # calculate fresh sha256
            self.sha256 = self._calculate_checksum(hashlib.sha256)

            # create an stub transient rec 
            self.transient_rec = {
                '_id': self.transient_id,
                'sha256': self.sha256,
                'filename': self.filename,
                'hostname':  self.app.conf['hostname']}

            # and fill up the transient record see if there are changes
            self.refresh()

        else:
            lg.debug('transient record found!')
            self.app.counter['transload'] += 1
            self.sha256 = self.transient_rec['sha256']

            # refrehs the transient record - see if there are changes
            self.refresh()

            # if something changed - recalc thte sha256
            if self.dirty:
                self.app.counter['transdirty'] += 1

                newsha256 = self._calculate_checksum(hashlib.sha256)
                if newsha256 == self.transient_rec['sha256']:
                    self.app.counter['sha256_ok'] += 1
                else:
                    self.app.counter['sha256_change!'] += 1
                    self.transient_rec['sha256'] = newsha256
                    # TODO: Create a transaction!!!

        self.core_rec = self.db.core.find_one({'_id': self.sha256})

        if self.core_rec is None:
            lg.debug('Core rec not found')
            self.core_rec = {'_id': self.sha256}
            pass
        else:
            for k, v in self.core_rec.items():
#                print('>>>', k, v)
                if k == '_id': continue
                if (k not in self.transient_rec) or \
                        (self.transient_rec[k] != v):
                    self.transient_rec[k] = v
                    self.dirty = True

        if self.dirty:
            lg.debug('dirty transient rec, saving')
            if getattr(self.app, 'bulk_mode', False):
                lg.debug('pepare bulk insert for {}'.format(self.filename))
                self.app.bulk_transient\
                    .find({'_id': self.transient_id})\
                    .upsert()\
                    .update({"$set": self.transient_rec})
            else:
                self.db.transient.insert_one(self.transient_rec)
            self.dirty=False

        self.app.run_hook('onload', self)


    def keys(self):
        """Return keys - madfile as a dictionary."""
        for k in self.transient_rec:
            yield k


    def __setitem__(self, rawkey, val):
        """Set value for this MF."""

        key, kinfo = key_info(self.app.conf, rawkey)
        val = kinfo['transformer'](val)
        setter = kinfo['setter']

        #set the transient record, 
        changed, setvalue = setter(self.transient_rec, key, val)


        lg.debug('Set "{}" = "{}" for {}"'.format(key, val, setvalue))


        if (key in self.core_rec) and (self.core_rec[key] == setvalue):
            #already in core
            pass
        else:
            self.core_rec[key] = setvalue
            changed = True

        if changed:
            self.dirty=True
            lg.debug('saving transient+rec for {}'.format(self.filename))
            self.save()


    def update(self, other):
        """Conform dict."""
        for k, v in other.items():
            if isinstance(v, list):
                for vv in v:
                    self[k] = vv
            else:
                self[k] = v

    def save(self):
        """Save record to mongodb."""

        lg.debug("Saving {} (bulk:{})".format(
            self.filename, getattr(self.app, 'bulk_mode', 'cantfind')))

        if getattr(self.app, 'bulk_mode', False):
            lg.debug('pepare bulk update/save for {}'.format(self.filename))
            self.app.bulk_transient\
                .find({'_id': self.transient_id})\
                .upsert().update({'$set': self.transient_rec})
            if len(self.core_rec) > 1:
                lg.debug('also bulk storing core {}'.format(self.filename))
                self.app.bulk_core\
                    .find({'_id': self.sha256})\
                    .upsert().update({'$set': self.core_rec})
        else:
            lg.debug('pepare normal update/save for {}'.format(self.filename))
            self.db.transient.update_one({'_id': self.transient_id},
                                         {'$set': self.transient_rec})
            if len(self.core_rec) > 1:
                self.db.core.update_one({'_id': self.sha256},
                                        {'$set': self.core_rec})
        self.dirty = False


    def __getitem__(self, key):
        """Return value from transient_rec."""
        return self.transient_rec[key]


    def refresh(self):
        """Refresh the transient record, and check if up to date.
        """
        statmap = dict(
            size=self.filestat[stat.ST_SIZE],
            nlink=self.filestat[stat.ST_NLINK],
            mtime=datetime.fromtimestamp(self.filestat[stat.ST_MTIME]),
            gid=self.filestat[stat.ST_GID],
            uid=self.filestat[stat.ST_UID],
            mode=self.filestat[stat.ST_MODE])

        for k, v in statmap.items():
            if k in self.transient_rec:
                if self.transient_rec[k] == v:
                    continue
                else:
                    lg.debug("transstat changed {}, {}".format(k, v))
                    self.transient_rec[k] = v
                    self.dirty = True
            else:
                lg.debug("transstat new {}, {}".format(k, v))    
                self.transient_rec[k] = v
                self.dirty = True

    def get_transient_id(self):
        """Transient id is a unqiue id for a file on a certain host. It is
        quick to calculate, but might change (without the underlying
        file changing. Features used to calculate the transient id
        are:

          - hostname
          - filename

        Technically,  these fields could also be added, but if the former 
        two are the same, it is very likely the same file. Otherwise we'll 
        pick up a change
   
          - filesize
          - last modification time

        if the transient id changes, the core id needs to be recalculated
        """
        sha256 = hashlib.sha256()
        sha256.update(self.app.conf['hostname'].encode('UTF8'))
        sha256.update(self.filename.encode('UTF8'))
        return sha256.hexdigest()


    def _calculate_checksum(self, hashobject):
        "Return the sha1sum for a certain filename - expected is a full path"
        h = hashobject()
        blocksize = 2 ** 20

        try:
            with open(self.filename, 'rb') as F:
                for chunk in iter(lambda: F.read(blocksize), b''):
                    h.update(chunk)
            return h.hexdigest()

        except IOError:
            # something went wrong reading the file (no permissions??
            # ignore)
            lg.warning("Cannot generate checksum for %s", filename)
            return None
