

import logging

import leip
import pymongo

from mad3.db import get_db
from mad3.util import persistent_cache, mongo_cache
from mad3.util import key_info, nicesize, nicenumber

lg = logging.getLogger(__name__)


def _single_sum(app, group_by=None, force=False):
    groupby_field = "${}".format(group_by)
    db = get_db(app)
    query = [{'$unwind': groupby_field},
             {'$group': {
                "_id": groupby_field,
                "total": {"$sum": "$size"},
                "count": {"$sum": 1}}},
            {"$sort": {"total": -1
                   }}]

    res = db.transient.aggregate(query)
    rv = list(res)
    return rv


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




FIND_WASTER_PIPELINE = [
    {"$project": {"filesize": 1,
                  "sha1sum": 1,
                  "username": 1,
                  "usage": {"$divide": ["$filesize", "$nlink"]}}},
    {"$group": {"_id": "$sha1sum",
                "user": {"$addToSet":  "$username"},
                "no_records": {"$sum": 1},
                "mean_usage": {"$avg": "$usage"},
                "total_usage": {"$sum": "$usage"},
                "filesize": {"$max": "$filesize"}}},
    {"$project": {"filesize": 1,
                  "total_usage": 1,
                  "user": 1,
                  "waste": {"$subtract": ["$total_usage", "$filesize"]}}},

    {"$sort": {"waste": -1}},
    {"$limit": 1000}]


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'waste'), 1,  24*60*60)
def _run_waste_command(app, name, force=False):
    """
    Execute mongo command.
    """
    MONGO_mad = get_mongo_transient_db(app)
    res = MONGO_mad.aggregate(FIND_WASTER_PIPELINE, allowDiskUse=True)
    return list(res)


@leip.flag('-N', '--no-color', help='no ansi coloring of output')
@leip.flag('--todb', help='save to mongo')
@leip.arg('-n', '--no-records', default=20, type=int)
@leip.flag('-f', '--force')
@leip.command
def waste(app, args):

    db = get_mongo_transient_db(app)

    res = _run_waste_command(app, 'waste_pipeline',
                             force=args.force)

    if args.todb:
        dbrec = {'time': datetime.datetime.utcnow(),
                 'data': res}
        db = mad2.util.get_mongo_db(app)
        db.waste.insert_one(dbrec)
        return

    def cprint_nocolor(*args, **kwargs):
        if 'color' in kwargs:
            del kwargs['color']
        if len(args) > 1:
            args = args[:1]
        print(*args, **kwargs)

    # if args.no_color:
    #     cprint = cprint_nocolor

    for i, r in enumerate(res):
        if i >= args.no_records:
            break

        sha1sum = r['_id']
        if not sha1sum.strip():
            continue

        cprint(sha1sum, 'yellow', end='')
        cprint(" sz ", "grey", end="")
        cprint("{:>9}".format(humansize(r['waste'])), end='')
        cprint(" w ", "grey", end="")
        cprint("{:>9}".format(humansize(r['filesize'])), end='')
        hostcount = collections.defaultdict(lambda: 0)
        hostsize = collections.defaultdict(lambda: 0)
        owners = set()
        for f in db.find({'sha1sum': sha1sum}):
            owners.add(f['username'])
            host = f['host']
            hostcount[host] += 1
            hostsize[host] += float(f['filesize']) / float(f['nlink'])

        for h in hostcount:
            print(' ', end='')
            cprint(h, 'green', end=':')
            cprint(hostcount[h], 'cyan', end="")

        cprint(" ", end="")
        cprint(", ".join(owners), 'red')
