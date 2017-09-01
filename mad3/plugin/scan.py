

import logging
import os
import math
from datetime import datetime
import time
import subprocess as sp
import sys
import leip

from mad3.madfile import MadFile
from mad3.db import get_db
from mad3.util import nicesize

lg = logging.getLogger(__name__)


def print_counter(c):
    """Print progress counter to screen."""
    def fmt(i):
        k, v = i
        if k.endswith('_sz'):
            return '{}:{}'.format(k, nicesize(v))
        else:
            return '{}:{}'.format(k, v)

    print('\r'
          + ' '.join(map(fmt, sorted(c.items())))
          + ' <<               ',
          end='')
    sys.stdout.flush()


@leip.flag('-q', '--quick', help='do not calculate shasums, do not store data '
           'in the core database')
@leip.flag('-r', '--refresh', help='refresh all files')
@leip.command
def scan(app, args):

    basedir = os.getcwd().rstrip('/') + '/'
    db = get_db(app)
    starttime = time.time()

    app.bulk_init()

    ff_regex = "^{}".format(basedir)

    lg.info("Query database for files below\n    {}".format(basedir))
    allfilesdb = db.transient.find({'filename': {"$regex": ff_regex}},
                                 projection=['filename', 'mtime', 'size'])

    file2id = {}
    allfiles = []
    for x in allfilesdb:
        allfiles.append((x['filename'], x['mtime'], x['size']))
        file2id[x['filename']] = x['_id']

    allfiles = set(allfiles)
    lg.info("Found {} files in db".format(len(allfiles)))


    madignore = os.path.expanduser('~/.madignore')
    cwd = os.path.abspath(os.path.normpath(os.getcwd()))
    cl = r"find {} -type f -printf '%p\t%T@\t%s\n'".format(cwd)

    if os.path.exists(madignore):
        cl += ' | grep -v -f ~/.madignore'

    lg.info('running unix find')
    P = sp.Popen(cl, shell=True, stdout=sp.PIPE, stderr=sp.DEVNULL)
    o, e = P.communicate()

    o = list(map(str.strip, o.decode().split('\n')))
    o = filter(None, o)

    def cnv2(l):
        p, m, s = l.rsplit("\t", 2)
        m = datetime.fromtimestamp(int(math.floor(float(m))))
        s = int(s)
        return (p, m, s)

    o = list(map(cnv2, o))
    lg.info("unix find found {} files".format(len(o)))
    now = set(o)

    if args.refresh:
        changed = list(now)
    else:
        changed = list(now - allfiles)

    deleted = list(allfiles - now)

    app.counter['indb'] = len(allfiles)
    app.counter['onfs'] = len(now)
    app.counter['check'] = len(changed)
    app.counter['rm'] = len(deleted)

    lg.info('in database       : {:>8d}'.format(len(allfiles)))
    lg.info('on filesystem     : {:>8d}'.format(len(now)))
    lg.info('total new/changed : {:>8d}'.format(len(changed)))
    lg.info('total deleted     : {:>8d}'.format(len(deleted)))

    for i, c in enumerate(sorted(changed)):
        lg.info('changed: {} path  {}'.format(i, c[0]))
        lg.info('           mtime {}'.format(c[1]))
        lg.info('           size  {}'.format(c[2]))
        if i > 3:
            break

    delids = [file2id[x[0]] for x in deleted]

    db.transient.remove({'_id': {"$in": delids}})

    changed = set([f[0] for f in changed])
    deleted = set([f[0] for f in deleted])


    lg.info("{} files seem changed".format(len(changed)))

    lastscreenupdate = time.time()
    # print_counter(app.counter)

    # store in database

    for filename in changed:

        app.counter['changed'] += 1
        try:
            mfile = MadFile(app, filename, quick=args.quick)
        except PermissionError as e:
            app.counter['noaccess'] += 1
            continue

        if mfile.dirty:
            mfile.save()

        if time.time() - lastscreenupdate > 2:
            print_counter(app.counter)
            lastscreenupdate = time.time()

    app.bulk_execute()

    print_counter(app.counter)
    # ensure we end on a newline
    print("\nruntime: {:.4f}".format(time.time() - starttime))



@leip.flag('-q', '--quick', help='do not calculate shasums, do not store data '
           'in the core database')
@leip.flag('-r', '--refresh', help='refresh all files')
@leip.command
def scan2(app, args):

    basedir = os.getcwd().rstrip('/') + '/'
    db = get_db(app)
    lastscreenupdate = starttime = time.time()
    app.bulk_init()

    ff_regex = "^{}".format(basedir)

    lg.info("Query database for files below\n    {}".format(basedir))
    allfilesdb = db.transient.find({'filename': {"$regex": ff_regex}},
                                 projection=['filename', 'mtime', 'size'])

    file2id = {}
    allfiles = []
    for x in allfilesdb:
        allfiles.append((x['filename'], x['mtime'], x['size']))
        file2id[x['filename']] = x['_id']

    allfiles = set(allfiles)
    # print(len(allfiles))
    lg.info("Found {} files in db".format(len(allfiles)))
    app.counter['indb'] = len(allfiles)
    app.message('Files in db: {}'.format(len(allfiles)))


    # run Unix FIND (seems the fastest way, plus we get the benefit
    # of adding in grep)


    madignore = os.path.expanduser('~/.madignore')
    cwd = os.path.abspath(os.path.normpath(os.getcwd()))
    cl = r"find {} -type f -printf '%p\t%T@\t%s\n'".format(cwd)

    if os.path.exists(madignore):
        cl += ' | grep -v -f ~/.madignore'

    lg.info('running unix find')

    def cnv2(l):
        p, m, s = l.rsplit("\t", 2)
        m = datetime.fromtimestamp(int(math.floor(float(m))))
        s = int(s)
        return (p, m, s)

    deleted = set()
    changed = set()
    onfs = set()


    P = sp.Popen(cl, shell=True, bufsize=1 stdout=sp.PIPE, stderr=sp.DEVNULL)
    with P.stdout as uxfind:
        for ii, line in enumerate(uxfind.readlines()):

            ffile = cnv2(line.strip().decode())
            if not ffile:
                continue

            onfs.add(ffile)
            app.counter['seenonfs'] += 1
            in_allfiles = ffile in allfiles
            if (in_allfiles and args.refresh) or \
                    (not in_allfiles):
                #needs refreshing

                filename = ffile[0]
                if in_allfiles:
                    app.counter['refresh'] += 1
                else:
                    app.counter['new'] += 1

                try:
                    mfile = MadFile(app, filename, quick=args.quick)
                except PermissionError as e:
                    app.counter['noxs'] += 1
                    continue

                if mfile.dirty:
                    mfile.save()

                if time.time() - lastscreenupdate > 2:
                    print_counter(app.counter)
                    lastscreenupdate = time.time()
            else:
                # ignoring this file, it exists, and has not changed
                app.counter['notnew'] += 1

    app.bulk_execute()

    deleted = list(allfiles - onfs)

    app.counter['onfs'] = len(onfs)
    app.counter['rm'] = len(deleted)
    delids = [file2id[x[0]] for x in deleted]

    db.transient.remove({'_id': {"$in": delids}})
    lg.info("{} files seem changed".format(len(changed)))

    print_counter(app.counter)
    # ensure we end on a newline
    print("\nruntime: {:.4f}".format(time.time() - starttime))
