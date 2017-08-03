

import logging

import leip
import pymongo

from mad3.db import get_db
from mad3.util import persistent_cache, mongo_cache
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


@leip.command
def truncate(app, args):
    """Drop all data from transient"""
    db = get_db(app)
    # db.transient.drop()
    # db.core.drop()
#    db.transaction.drop()

@leip.flag('-H', '--human', help='human readable')
@leip.flag('-f', '--force')
@leip.command
def allkeys(app, args):

    def get_allkeys():
        import bson.code
        mapper = bson.code.Code('''
            function () { for (var key in this) {
                emit(key, {count: 1, size: this.size}); };
            }''')
        reducer = bson.code.Code('''
            function(key, values) {
                result = {count: 0, size: 0};
                values.forEach(function(value) {
                    result.count += value.count;
                    result.size += value.size; });
                return result; }''')
        db = get_db(app)
        result = db.transient.map_reduce(mapper, reducer, out="_allkeys")
        rv = [k for k in result.find()]
        rv.sort(key=lambda x: x['value']['size'], reverse=True)
        db['_allkeys'].drop()
        return rv

    res =  mongo_cache(app, get_allkeys, 'allkeys', 60*60*24, force=args.force)

    if args.human:
        klen = max([len(x['_id']) for x in res])
        fms = "{:" + str(klen) + "}\t{:>10}\t{:>9}"
        for d in res:
            print(fms.format(d['_id'], nicesize(int(d['value']['size'])),
                             nicenumber(int(d['value']['count']))))

    else:
        for d in res:
            print(d['_id'], int(d['value']['count']), int(d['value']['size']), sep="\t")

@leip.flag('-f', '--force', help='force query (otherwise use cache, and'
           ' query only once per day')
@leip.flag('-H', '--human', help='human readable')
@leip.arg('key', nargs='?')
@leip.command
def sum(app, args):
    """
    Show the associated mongodb record
    """
    if not args.key:
        db = get_db(app)
        notrans = db.transient.count()
        print("No Transient records: ", notrans)
        if notrans > 0:
            print("Total data Transient: ", nicesize(
                list(db.transient.aggregate([
                    {"$group": {"_id": None,
                                "total": {"$sum": "$size"}}}]))[0]['total']))
        print("     No Core records: ", db.transient.count())
        return

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
