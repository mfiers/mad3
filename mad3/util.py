
import logging
import os
import pickle


import mad3.db


lg = logging.getLogger(__name__)


datatypes = dict(
    str=str,
    int=int,
    float=float )


def setone(data, k, v):
    """Simply set a value, and check if it has changed."""
    if (k in data) and (data[k] == v):
        return False, v
    data[k] = v
    return True, v


def setset(data, k, v):
    """Add a value to a set, and check if it has changed."""

    if not isinstance(v, list):
        v = [v]

    if k in data:
        #print#('found', k, data[k], isinstance(data[k], list))
        if not isinstance(data[k], list):
            data[k] = [data[k]]
        changed = False
        for vv in v:
            if vv not in data[k]:
                changed = True
                data[k].append(vv)
        return changed, data[k]
    else:
        data[k] = v
        return True, data[k]


datasetter = dict(one=setone,
                  set=setset)


def key_info(conf, key):

    info = conf['keywords'].get(key)

    if info is None:
        lg.warning("Bad key: {}".format(key))
        exit(-1)

    if info is None:
        return info

    if 'alias' in info:
        return key_info(conf, info['alias'])

    info['shape'] = str(info.get('shape', 'one'))
    info['type'] = info.get('type', 'str')
    info['transformer'] = datatypes.get(info['type'], str)
    info['setter'] = datasetter.get(info['shape'], setone)
    return key, info


def nicenumber(val):
    """Return a number with , separators for the thousands."""
    val = str(val)[::-1]
    rv = []
    for i in range(0, len(val), 3):
        rv.append(val[i:i+3][::-1])
    return ','.join(rv[::-1])


def nicesize(val ,precision=2):
    """Convert a number to a nicer readable number with Tb/Gb postfixes."""
    for pw, metric in [(24, 'Y'), (21, 'Z'), (18, 'E'),
                   (15, 'P'), (12, 'T'), (9, 'G'),
                   (6, 'M'), (3, 'K')]:
        if val > (10 ** pw):
            formatstring = '{{:.{}f}} {}b'.format(precision, metric)
            return formatstring.format(val * (10 ** -pw))
    return str(val)


def persistent_cache(path, cache_on, duration):
    """
    Disk persistent cache that reruns a function once every
    'duration' no of seconds
    """
    def decorator(original_func):

        def new_func(*args, **kwargs):

            if isinstance(cache_on, str):
                cache_name = kwargs[cache_on]
            elif isinstance(cache_on, int):
                cache_name = args[cache_on]

            full_cache_name = os.path.join(path, cache_name)
            lg.debug("cache file: %s", full_cache_name)
            run = False

            if kwargs.get('force'):
                run = True

            if not os.path.exists(full_cache_name):
                #file does not exist. Run!
                run = True
            else:
                #file exists - but is it more recent than
                #duration (in seconds)
                mtime = os.path.getmtime(full_cache_name)
                age = time.time() - mtime
                if age > duration:
                    lg.debug("Cache file is too recent")
                    lg.debug("age: %d", age)
                    lg.debug("cache refresh: %d", duration)
                    run = True

            if not run:
                #load from cache
                lg.debug("loading from cache: %s", full_cache_name)
                with open(full_cache_name, 'rb') as F:
                    try:
                        res = pickle.load(F)
                        return res
                    except EOFError:
                        lg.warning("problem loading cached object")
                        os.unlink(full_cache_name)


            #no cache - create
            lg.debug("no cache - running function %s", original_func)
            rv = original_func(*args, **kwargs)
            lg.debug('write to cache: %s', full_cache_name)

            if not os.path.exists(path):
                os.makedirs(path)
            try:
                with open(full_cache_name, 'wb', pickle.HIGHEST_PROTOCOL) as F:
                    pickle.dump(rv, F)
            except:
                print(rv)
                raise

            return rv

        return new_func

    return decorator

