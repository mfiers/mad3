"""
Relation functions for Mad3
"""
# noqa: T484

import argparse
import hashlib
import logging
import os
import re
import subprocess as sp
from typing import Type
import sys

import jinja2
import leip

from mad3.db import get_db
from mad3.util import nicedictprint
from mad3.util import nicetimedelta
from mad3.madfile import MadFile
import mad3.relation

lg = logging.getLogger(__name__)


class Template:
    """Executable template."""

    finditem = re.compile((r'\[\[([><\?])\s*(\w+)(.*?)\s*\]\]'))

    def __init__(self,
                 app: Type[leip.app],
                 name: str,
                 template_args: dict) -> None:
        """Instantiate template."""
        # Find and read the template file
        tfile = os.path.expanduser('~/m3/' + name + '.m3t')
        with open(tfile) as F:
            self.raw_template = F.read()

        self.app = app
        self.name = name
        self.template_args = template_args

        # Contains information on the template variables
        self.data = {}             # type: dict

        # Ultimately contains the determined values of the template vars
        self.variables = {}        # type: dict

        # ensure a unique id for the template
        template_sha256 = hashlib.sha256()
        template_sha256.update(self.raw_template.encode())
        self.variables['template_sha256'] = template_sha256.hexdigest()
        # relation object represents what happens in the m3 system
        self.relation = mad3.relation.Relation(self.app)
        self.relation.init()

    def prepare(self):
        """Parse & render template."""
        self.parse_template()
        self.build_argparser()
        self.parse_arguments()
        self.render_template()
        self.update_relation()

    def checkrun(self):
        """Check whether we should run."""
        return self.relation.check(action='message')

    def find_in_db(self, *args, **kwargs):
        """Check whether we should run."""
        return self.relation.find_in_db(*args, **kwargs)

    def refresh_io(self, *args, **kwargs):
        """Refresh IO."""
        return self.relation.refresh_io(*args, **kwargs)

    def parse_template(self):
        """Parse the template string."""
        for line in self.raw_template.split("\n"):
            line = line.strip()
            if line.startswith('#m3'):
                key, val = line[3:].strip().split('=', 1)
                key = key.strip()
                val = val.strip()
                self.variables[key] = val

        for fitem in self.finditem.finditer(self.raw_template):
            fgrp = fitem.groups()
            categ = fgrp[0]
            name = fgrp[1]
            rest_str = fgrp[2]
            rest = {}     # type: dict
            for item in rest_str.split('|'):
                item = item.strip()
                if item:
                    key, val = item.split('=')
                    rest[key] = val

            self.data[name] = (categ, rest)

    def build_argparser(self):
        """Build the argument parser."""
        firstletters = ''
        for name, (categ, rest) in self.data.items():
            firstletters += name[0]

        self.argparser = argparse.ArgumentParser(
            usage='m3 x {} [arguments]'.format(self.name))

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
                self.argparser.add_argument('-' + name[0],
                                    '--' + name, **argargs)  # noqa: T484
            else:
                self.argparser.add_argument('--' + name, **argargs) # noqa:T484

    def parse_arguments(self):
        """Actually parse and process the passed arguments."""
        self.args = self.argparser.parse_args(self.template_args)  # noqa: T484

        # get values from args or defaults
        for name, (categ, rest) in self.data.items():
            if categ not in '<>?':
                continue
            val = getattr(self.args, name)
            if rest.get('type') == 'flag':
                val = str(rest.get('val')) if val else ''
            else:
                val = val if val is not None else rest.get('default')
            self.variables[name] = val

        # possibly fill in substitutions in the template variables
        findreplace = re.compile(r'{{\s*(\w+)\s*}}')
        for name, val in self.variables.items():
            if findreplace.search(val):
                t = jinja2.Template(val)
                self.variables[name] = t.render(self.variables)

    def render_template(self):
        """Render the raw template into a run-ready script."""
        # create and expand commandline template
        tmpl_r1 = self.finditem.sub(r'{{\2}}', self.raw_template)
        tmpl_r2 = jinja2.Template(tmpl_r1).render(self.variables)
        self.relation.script = tmpl_r2
        self.relation.template_sha256 = self.variables['template_sha256']

    def update_relation(self):
        """Register the IO with the relation record."""
        for name, (categ, rest) in self.data.items():
            if categ not in '<>':
                continue
            filename = self.variables[name]
            catname = 'input' if categ == '<' else 'output'
            self.relation.add_io(catname, filename)
        if 'executable' in self.variables:
            self.relation.add_executable(
                self.variables['executable'])


    @property
    def script(self):
        """Script responsible for this template/relation."""
        return self.relation.script

    def execute(self):
        """Execute the script."""
        sp.call(self.script, shell=True)

    def save(self):
        """Save the relation."""
        self.relation.save()


@leip.arg('template')
@leip.partial
@leip.nohelp
@leip.command
def x(app, args):
    """Apply template."""
    xargs = app.xargs
    tmpl = Template(app, args.template, xargs)

    tmpl.prepare()
    tstate = tmpl.checkrun()
    print(tstate)
    tmpl.execute()
    tmpl.refresh_io()

    nicedictprint(tmpl.relation.data)
    tmpl.save()


@leip.arg('template_args', nargs='*')
@leip.arg('template')
@leip.command
def tmap(app, args):
    """Apply relation template to a bunch of files."""
    tmpl = Template(app, args.template, args.template_args)
    for a in args.template_args:
        print(a)
#    sp.call(tmpl.script, shell=True)#
#    from pprint import pprint
#    pprint(tmpl.relation.data)


@leip.flag('-H', '--human', help='output human friendly relation records')
@leip.arg('-s', '--state')
@leip.arg('-t', '--hostname')
@leip.arg('-i', '--id')
@leip.arg('-f', '--file', help='Involving this file')
@leip.command
def tfind(app: Type[leip.app],
          args: Type[argparse.Namespace]):
    """Find a relation."""
    db = get_db(app)
    transact = db['relation']
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
        query['io.sha256'] = sha256
        # print(madfile, sha256)

    simple_query('hostname')
    simple_query('state')

    for r in transact.find(query):
        if args.human:
            print("{:>6s} {} {:8} {}:{}".format(
                nicetimedelta(r['time']),
                r['_id'][:10], r['state'],
                r['hostname'], r['pwd'], ))
            for io in sorted(r['io'], key=lambda x: x['category']):
                marker = '<i' if io['category'] == 'input' else 'o>'
                print('  {} {} ({})'.format(marker, io['filename'],
                                            io['sha256'][:8]))
        else:
            nicedictprint(r)


@leip.arg('-c', '--script', help='Script to run (- == stdin)')
@leip.arg('-i', '--input', action='append',
          help='input file(s) for the relation: [group::]filename')
@leip.arg('-o', '--output', action='append',
          help='output file(s) for the relation: [group::]filename')
@leip.arg('-s', '--state', help='relation state',
          choices='pending running finished'.split(),
          default='finished')
@leip.command
def tadd(app: Type[leip.app],
         args: Type[argparse.Namespace]):
    """Add a relation."""
    relation = mad3.relation.relation(app)
    relation.init()
    relation.state = args.state    # noqa: T484

    # determine script
    if getattr(args, 'script'):
        script = args.script    # noqa: T484
        if script == '-':
            relation.script = sys.stdin.read()
        elif os.path.exists(script):
            relation.script = open(script).read()
        else:
            app.warning('Cannot find cl: {}'.format(script))

    for cat in 'input output'.split():
        if cat in args:    # noqa: T484
            for val in getattr(args, cat):
                if '::' in val:
                    group, name = val.split('::')
                else:
                    group, name = cat, val
                relation.add_io(category=cat,
                                   filename=name,
                                   group=group)
