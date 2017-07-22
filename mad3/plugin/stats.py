

import logging

import leip

from mad3.db import get_db
from mad3.util import persistent_cache
from mad3.util import key_info, nicesize, nicenumber

lg = logging.getLogger(__name__)


#@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'sum'), 'group_by', 62400)
def _single_sum(app, group_by=None, force=False):
    groupby_field = "${}".format(group_by)
    db = get_db(app)

    query = [{'$group': {
                "_id": groupby_field,
                "total": {"$sum": "$size"},
                "count": {"$sum": 1}}},
             {'$unwind': "$_id"},
            {"$sort": {"total": -1
                   }}]
    res = db.transient.aggregate(query)
    rv = list(res)
    return rv



@leip.flag('-f', '--force', help='force query (otherwise use cache, and'
           ' query only once per day')
@leip.flag('-H', '--human', help='human readable')
@leip.arg('key', default='investigation')
@leip.command
def sum(app, args):
    """
    Show the associated mongodb record
    """
    kname, kinfo = key_info(app.conf, args.key)
    res = _single_sum(app, group_by=kname, force=args.force)
    total_size = int(0)
    total_count = 0
    mgn = len("Total")
    for reshost in res:
        gid = reshost['_id']
        if gid is None:
            mgn = max(4, mgn)
        else:
            mgn = max(len(str(reshost['_id'])), mgn)

    fms = "{:" + str(mgn) + "}\t{:>10}\t{:>9}"
    if args.human:
        print("# {}:".format(kname))
    for reshost in res:
        total = reshost['total']
        count = reshost['count']
        total_size += int(total)
        total_count += count
        if args.human:
            total_human = nicesize(total)
            count_human = nicenumber(count)
            categ = reshost['_id']
            if categ is None:
                categ = "<undefined>"
            print(fms.format(
                categ, total_human, count_human))
        else:
            print("{}\t{}\t{}".format(
                reshost['_id'], total, count))

    if args.human:
        total_size_human = nicesize(total_size)
        total_count_human = nicenumber(total_count)
        print(fms.format('', '-'*10, '-'*9))
        print(fms.format(
            "Total", total_size_human, total_count_human))
    else:
        print("Total\t{}\t{}".format(total_size, total_count))
