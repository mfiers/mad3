"""
Transaction functions for Mad3
"""

import argparse
from datetime import datetime
import logging
import os
import re
import subprocess as sp
from typing import Type
import sys

import jinja2
import leip
import yaml


from mad3.db import get_db
from mad3.util import get_random_sha256, nicedictprint

from mad3.madfile import MadFile

lg = logging.getLogger(__name__)


class Transaction:
    """A Transaction represents a relationship between files."""

    def __init__(self, app: Type[leip.app]) -> None:
        """Initialize the Transcation method."""
        self.app = app
        self.data = {}    # type: dict

    def init(self) -> None:
        """Initialze the transaction with default variables."""
        self.data = dict(
            _id=get_random_sha256(),
            time=datetime.now(),
            pwd=os.getcwd(),
            state='pending',
            hostname=self.app.conf['hostname'])

    def save(self):
        """Save transaction to the database."""
        db = get_db(self.app)
        transact = db['transaction']
        transact.insert(self.data)

    @property
    def state(self):
        """Transaction state."""
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
        """Script responsible for this transaction."""
        return self.data.get('script')

    @script.setter
    def script(self, new):
        self.data['script'] = new

    def add_io(self, category, filename, group=None):
        """Set an IO field in a transaction."""
        if group is None:
            group = category

        filename = os.path.abspath(os.path.expanduser(filename))

        # add the filename to the transaction data

        to_add = dict(
            category=category,
            group=group,
            filename=filename
        )

        if os.path.exists(filename):
            mf = MadFile(self.app, filename)
            to_add['sha256'] = mf.sha256

        if 'io' not in self.data:
            self.data['io'] = []
        self.data['io'].append(to_add)


class Template:
    """Transaction template."""

    def __init__(self,
                 app: Type[leip.app],
                 name: str,
                 template_args: dict) -> None:
        """Instantiate transaction template."""
        tfile = os.path.expanduser('~/m3/' + name + '.m3t')
        with open(tfile) as F:
            tmpl = F.read()

        self.app = app
        self.transaction = Transaction(self.app)
        self.data = {}          # type: dict
        self.tmpl = tmpl        # type: str
        self.values = {}        # type: dict

        # prepare transaction
        self.transaction = Transaction(app)
        self.transaction.init()
        self.transaction

        finditem = re.compile((r'\[\[([><\?])\s*(\w+)(.*?)\s*\]\]'))
        for fitem in finditem.finditer(tmpl):
            fgrp = fitem.groups()
            categ = fgrp[0]
            name = fgrp[1]
            rest = {}     # type: dict
            for item in fgrp[2].split('|'):
                item = item.strip()
                if item:
                    key, val = item.split('=')
                    rest[key] = val
            self.data[name] = (categ, rest)

        # build argparse
        parser = argparse.ArgumentParser(usage='template arguments')
        firstletters = [x[0] for x in self.data.keys()]
        for name, (categ, rest) in self.data.items():
            argargs = {}
            if rest.get('help'):
                argargs['help'] = rest['help']
            if rest.get('type') == 'flag':
                argargs['action'] = 'store_true'
                argargs['required'] = False
            elif 'default' not in rest:
                argargs['required'] = True
            if firstletters.count(name[0]) == 1:
                parser.add_argument('-' + name[0],
                                    '--' + name, **argargs)  # noqa: T484
            else:
                parser.add_argument('--' + name, **argargs)  # noqa: T484

        args = parser.parse_args(template_args)  # noqa: T484

        # get values from args or defaults
        for name, (categ, rest) in self.data.items():
            val = getattr(args, name)
            if rest.get('type') == 'flag':
                val = str(rest.get('val')) if val else ''
            else:
                val = val if val is not None else rest.get('default')
            self.values[name] = val

        # possibly fill in substitutions &
        # recognize_template
        findreplace = re.compile(r'{{\s*(\w+)\s*}}')
        for name, val in self.values.items():
            if findreplace.search(val):
                t = jinja2.Template(val)
                self.values[name] = t.render(self.values)

        # create and expand commandline template
        tmpl_r1 = finditem.sub(r'{{\2}}', self.tmpl)
        tmpl_r2 = jinja2.Template(tmpl_r1).render(self.values)
        self.transaction.script = tmpl_r2

        for name, (categ, rest) in self.data.items():
            if categ not in '<>':
                continue
            filename = self.values[name]
            catname = 'input' if categ == '<' else 'output'
            self.transaction.add_io(catname, filename)

    @property
    def script(self):
        """Script responsible for this template/transaction."""
        return self.transaction.script

    def execute(self):
        """Execute the script."""
        sp.call(self.script, shell=True)

    def save(self):
        """Save the transaction."""
        self.transaction.save()


@leip.arg('template_args', nargs='*')
@leip.arg('template')
@leip.command
def tap(app, args):
    """Apply transaction template."""
    tmpl = Template(app, args.template, args.template_args)
    tmpl.execute()
    nicedictprint(tmpl.transaction.data)
    tmpl.save()


@leip.arg('template_args', nargs='*')
@leip.arg('template')
@leip.command
def tmap(app, args):
    """Apply transaction template to a bunch of files."""
    tmpl = Template(app, args.template, args.template_args)
    for a in args.template_args:
        print(a)
#    sp.call(tmpl.script, shell=True)#
#    from pprint import pprint
#    pprint(tmpl.transaction.data)


@leip.arg('-s', '--state')
@leip.arg('-H', '--hostname')
@leip.arg('-i', '--id')
@leip.arg('-f', '--file', help="Involving this file")
@leip.command
def tfind(app: Type[leip.app],
          args: Type[argparse.Namespace]):
    """Find a transaction."""
    db = get_db(app)
    transact = db['transaction']
    argsd = vars(args)  # noqa: T484
    query = {}

    def simple_query(key):
        if key in argsd and argsd[key]:
            query[key] = argsd[key]

    # id is a special case
    if argsd.get('id'):
        tid = argsd['id']
        query['_id'] = tid if len(tid) == 64 \
            else {'$regex': '^{}'.format(tid)}

    filename = argsd.get('file')
    if filename:
        madfile = MadFile(app, filename)
        sha256 = madfile.sha256
        print(madfile, sha256)

    simple_query('hostname')
    simple_query('state')
    print(query)
#    for r in transact.find(query):#
#        nicedictprint(r)


@leip.arg('-c', '--script', help='Script to run (- == stdin)')
@leip.arg('-i', '--input', action='append',
          help='input file(s) for the transaction: [group::]filename')
@leip.arg('-o', '--output', action='append',
          help='output file(s) for the transaction: [group::]filename')
@leip.arg('-s', '--state', help='transaction state',
          choices='pending running finished'.split(),
          default='finished')
@leip.command
def tadd(app: Type[leip.app],
         args: Type[argparse.Namespace]):
    """Add a transaction."""
    transaction = Transaction(app)
    transaction.init()
    transaction.state = args.state    # noqa: T484

    # determine script
    if getattr(args, 'script'):
        script = args.script    # noqa: T484
        if script == '-':
            transaction.script = sys.stdin.read()
        elif os.path.exists(script):
            transaction.script = open(script).read()
        else:
            app.warning('Cannot find cl: {}'.format(script))

    for cat in 'input output'.split():
        if cat in args:    # noqa: T484
            for val in getattr(args, cat):
                if '::' in val:
                    group, name = val.split('::')
                else:
                    group, name = cat, val
                transaction.add_io(category=cat,
                                   filename=name,
                                   group=group)
