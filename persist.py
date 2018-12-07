import hashing
import pickling
import preprocessing
import diskcache
from functools import update_wrapper


def persist(func=None,
            basedir='persist',
            funcdir=None,
            key=None,
            storekey=False,
            pickle=pickling.pickle,
            unpickle=pickling.unpickle,
            hash=hashing.hash):

    class persist_wrapper:

        def __init__(self, func):
            update_wrapper(self, func)
            self._func = func
            self._hash = hash
            self._pickle = pickle
            self._unpickle = unpickle
            if key is None:
                self._key = self.default_key
            else:
                self._key = key
            if storekey:
                constr = diskcache.DiskCacheWithKeys
            else:
                constr = diskcache.DiskCache
            self.cache = constr(self, basedir, funcdir, storekey)

        def __call__(self, *args, **kwargs):
            key = self._key(*args, **kwargs)
            try:
                val = self.cache[key]
            except KeyError:
                val = self._func(*args, **kwargs)
                self.cache[key] = val
            return val

        def default_key(self, *args, **kwargs):
            return preprocessing.arg_tuple(self._func, *args, **kwargs)

        def clear(self):
            self.cache.clear()

    if func is None:
        # @persist(...)
        return persist_wrapper
    else:
        # @persist
        return persist_wrapper(func)


class PersistError(Exception):
    """Exception for errors to do with persistent memoisation"""
