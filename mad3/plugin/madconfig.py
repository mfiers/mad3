
import os
import copy
import yaml

import leip

from mad3.util import key_info#


MADFILEDATA = {}


def get_madfile_data(app, mfile):

    basedir = os.path.dirname(mfile.filename)
    if basedir in MADFILEDATA:
        return MADFILEDATA[basedir]

    parsedir = copy.copy(basedir)

    madfiles = []

    # find mad configuration files
    while True:
        madfile = '{}/{}'.format(parsedir, 'mad.config')
        if os.path.exists(madfile):
            madfiles.append(madfile)
        parsedir = os.path.dirname(parsedir)
        if parsedir == '/':
            break

    # load madfiles
    madfiledata = {}
    get_madfile_data
    for madfile in madfiles[::-1]:
        with open(madfile) as F:
            mdata = yaml.load(F)
            madfiledata.update(mdata)

    MADFILEDATA[basedir] = madfiledata

    return madfiledata


@leip.arg('value')
@leip.arg('key')
@leip.command
def dset(app, args):
    key, info = key_info(app.conf, args.key)
    value = info['transformer'](args.value)
    d = {}
    if os.path.exists('./mad.config'):
        with open('mad.config') as F:
            d = yaml.load(F)

    info['setter'](d, key, value)

    with open('mad.config', 'w') as F:
        yaml.dump(d, F, default_flow_style=False)


@leip.hook('onload')
def add_madfile_data(app, mfile):
    mfile.update(get_madfile_data(app, mfile))
