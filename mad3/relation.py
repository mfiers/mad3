"""
relations.
"""

from datetime import datetime
import logging
import os
import subprocess as sp
from typing import Type

import leip

from mad3.db import get_db
from mad3.madfile import MadFile

from mad3.util import get_random_sha256, nicedictprint

lg = logging.getLogger(__name__)

# signals
NO_INPUT_OUTPUT_DATA = 000
FILE_NOT_FOUND = 100
INPUT_FILE_NOT_FOUND = 101
OUTPUT_FILE_NOT_FOUND = 102
FILE_CHANGED = 200
INPUT_FILE_CHANGED = 201
OUTPUT_FILE_CHANGED = 202
DISCOVERED_DB_RECORD = 300
DISCOVERED_DB_RECORD_OUTPUT_CHANGED = 301


# Signal_messages
MESSAGES = {
    NO_INPUT_OUTPUT_DATA: 'No IO data found',
    FILE_NOT_FOUND: None,
    INPUT_FILE_NOT_FOUND: 'Input file not found: {filename}',
    OUTPUT_FILE_NOT_FOUND: 'Output file not found: {filename}',
    FILE_CHANGED: None,
    INPUT_FILE_CHANGED: 'Input file changed: {filename}',
    OUTPUT_FILE_CHANGED: 'Output file chaned: {filename}',
    DISCOVERED_DB_RECORD: 'Found overlapping db record',
    DISCOVERED_DB_RECORD_OUTPUT_CHANGED:
        'Found overlapping db record, but output changed',
}


class Relation:
    """A relation between two Mad files/objects."""

    def __init__(self,
                 app: Type[leip.app],
                 record: dict=None,
                 ) -> None:

        """Initialize the Transcation method."""
        self.app = app
        if record is not None:
            self.data = record
        else:
            self.data = {}    # type: dict


    def init(self) -> None:
        """Initialze the relation with default variables."""
        self.data = dict(
            _id=get_random_sha256(),
            time=datetime.now(),
            pwd=os.getcwd(),
            state='pending',
            hostname=self.app.conf['hostname'])

    def save(self):
        """Save relation to the database."""
        db = get_db(self.app)
        transact = db['relation']
        transact.insert(self.data)

    def find_in_db(self, strict=False):
        """
        Attempt to find a similar transcation in the db.

        Subsequently, load the shasum data from the database
        """
        query = [{'template_sha256': self.template_sha256}]  # build a pymongo query
        if 'io' not in self.data:
            self.app.warning('Cannot query db for a relation '
                             'with no IO files')
            return []

        rv = []

        for io in self.data['io']:
            if not io.get('sha256'):
                lg.info('Can\'t query, no input shasum for {}'
                        .format(io['filename']))
                return []

            query.append({
                'io': {'$elemMatch': {'sha256': io['sha256'],
                                      'category': io['category'],
                                      'group': io['group']}}})

        query = {'$and': query}
        db = get_db(self.app)
        # nicedictprint(query)
        for r in db.relation.find(query):
            rv.append(r)
        return rv

    def add_executable(self, execname):
        """Find and register an executable as input file."""
        if not os.path.exists(execname):
            # Try to find the exec using the shell tool `which`
            rv = sp.check_output('which {}'.format(execname), shell=True)
            rv = rv.decode().strip()
            if rv:
                self.add_io(category='input',
                            filename=rv,
                            group='executable')

    def check(self, action='message'):
        """Determine if this relation has already executed.

        Check two things:

        *) does the info in the current record match with what
           is on disk?
        *) is there a record in the db that matches with this
           record?
        """
        tstate = set()

        def observe(signal, io):
            """Observe something having changed."""
            tstate.add(signal)
            if action == 'message':
                message = MESSAGES[signal]
                if message is not None:
                    self.app.message(message, **io)

        if 'io' not in self.data:
            # no files? no way to see if this has already been done
            observe(NO_INPUT_OUTPUT_DATA, {})
            return tstate

        for io in self.data['io']:
            if not os.path.exists(io['filename']):
                observe(FILE_NOT_FOUND, io)
                if io['category'] == 'input':
                    observe(INPUT_FILE_NOT_FOUND, io)
                else:
                    observe(OUTPUT_FILE_NOT_FOUND, io)
            else:
                mf = MadFile(self.app, io['filename'])
                if mf.sha256 != io['sha256']:
                    observe(FILE_CHANGED, io)
                    if io['category'] == 'input':
                        observe(INPUT_FILE_CHANGED, io)
                    else:
                        observe(OUTPUT_FILE_CHANGED, io)
                else:
                    # unchanged sha256 - ignore
                    # no reason to not perform this relation
                    pass

        for dbrec in self.find_in_db():
            dbtra = Relation(self.app, dbrec)

            if not self.same_io_structure(dbtra):
                # Different input/output fields, not the same
                self.app.warning('Should not happen: different IO stucture')
                continue

            if not self.same_input(dbtra):
                # different input files: not the same
                self.app.warning('Should not happen: different input')
                continue

            if self.same_output(dbtra):
                observe(DISCOVERED_DB_RECORD, dbtra.data)
            else:
                print('changed output')
                observe(DISCOVERED_DB_RECORD_OUTPUT_CHANGED,
                        dbtra.data)

        return tstate

    def _get_io_struct(self):
        """Return array with structure of the io (cat/groups)."""
        rv = []
        for io in self.data['io']:
            rv.append((io['category'], io['group']))
        rv.sort()
        return rv

    def same_io_structure(self, other):
        return self._get_io_struct() == other._get_io_struct()

    def same_input(self, other):
        """Does the other record have the same input as this record?"""
        for io in other.data['io']:
            if io['category'] != 'input':
                continue
            if io not in self:
                return False
        return True

    def same_output(self, other):
        """Does the other record have the same input as this record?"""
        for io in other.data['io']:
            if io['category'] != 'output':
                continue
            if io not in self:
                return False
        return True

    def __contains__(self, other):
        """Is a IO record present in this record?"""
        for rec in self.data['io']:
            if rec['category'] != other['category']:
                continue
            if rec['group'] != other['group']:
                continue
            if rec['sha256'] != other['sha256']:
                continue
            return True
        return False

    @property
    def state(self):
        """relation state."""
        return self.data.get('state')

    @state.setter
    def state(self, new):
        self.data['state'] = new

    @property
    def time(self):
        """Time stamp."""
        return self.data.get('time')

    @time.setter
    def time(self, new):
        self.data['time'] = new

    @property
    def script(self):
        """Script responsible for this relation."""
        return self.data.get('script')

    @script.setter
    def script(self, new):
        self.data['script'] = new

    @property
    def template_sha256(self):
        """Sha256 identifying the template string."""
        return self.data.get('template_sha256')

    @template_sha256.setter
    def template_sha256(self, new):
        self.data['template_sha256'] = new

    def refresh_io(self):
        """top

        Recheck the IO.

        Warn if input has changed, update the record if the output
        has changed.
        """
        if 'io' not in self.data:
            return

        for io in self.data['io']:
            if os.path.exists(io['filename']):
                mf = MadFile(self.app, io['filename'])
                if io['category'] == 'input':
                    if mf.sha256 != io['sha256']:
                        raise Exception(
                            'Input file changed {}'
                            .format(io['filename']))
                elif io['category'] == 'output':
                    if 'sha256' in io and \
                            mf.sha256 != io['sha256']:
                        lg.warning('output sha256 changed {}'
                                   .format(io['filename']))
                    io['sha256'] = mf.sha256

    def add_io(self, category, filename, group=None):
        """Set an IO field in a relation."""
        if group is None:
            group = category

        # ensure there is an 'io' field
        if 'io' not in self.data:
            self.data['io'] = []

        # get the full expanded filename
        filename = os.path.abspath(os.path.expanduser(filename))

        # add the filename to the relation data
        to_add = dict(
            category=category,
            group=group,
            filename=filename
        )

        # if the file exists, ensure we have the sha256
        if os.path.exists(filename):
            mf = MadFile(self.app, filename)
            to_add['sha256'] = mf.sha256

        self.data['io'].append(to_add)
