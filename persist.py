import hashing
import pickling
import preprocessing
from functools import update_wrapper
from os import makedirs, remove, listdir
from os.path import exists, join
from re import compile
from collections.abc import MutableMapping, Iterator


def persist(func=None,
            basedir='persist',
            funcdir=None,
            pickle=pickling.pickle,
            unpickle=pickling.unpickle,
            hash=hashing.hash,
            key=None,
            storekey=False):

    class persist_wrapper:

        def __init__(self, func):
            update_wrapper(self, func)
            self._func = func
            self._hash = hash
            self._basedir = basedir
            if funcdir is None:
                self._funcdir = func.__name__
            else:
                self._funcdir = funcdir
            self._dir = join(self._basedir, self._funcdir)
            if not exists(self._dir):
                makedirs(self._dir)
            self._pickle = pickle
            self._unpickle = unpickle
            if key is None:
                self._key = self.default_key
            else:
                self._key = key
            if storekey:
                self.cache = self.DiskCacheWithKeys(self, self._dir)
            else:
                self.cache = self.DiskCache(self, self._dir)

        def __call__(self, *args, **kwargs):
            key = self._key(*args, **kwargs)
            h = self._hash(key)
            fname = self.filename(h)
            if exists(fname):
                print(f'''Retrieving cached value for {key}''')
                print(f'''Reading from {fname}''')
                file = open(fname, 'r')
                if storekey:
                    storedkey = self._unpickle(file.readline().rstrip('\n'))
                    if storedkey == key:
                        print('Key verified')
                    else:
                        raise PersistError(storedkey, key)
                val = self._unpickle(file.read())
                file.close()
            else:
                print(f'''Computing value for {key}''')
                val = self._func(*args, **kwargs)
                print(f'''Writing to {fname}''')
                file = open(fname, 'w')
                if storekey:
                    file.write(self._pickle(key))
                    file.write('\n')
                file.write(self._pickle(val))
                file.close()
            return val

        def default_key(self, *args, **kwargs):
            return preprocessing.arg_tuple(self._func, *args, **kwargs)

        def filename(self, h):
            return '%s/%s.out' % (self._dir, h)

        def clear(self):
            for f in listdir(self._dir):
                path = join(self._dir, f)
                # TODO: safety checks?
                remove(path)


        class DiskCache:
            def __init__(self, func, dir):
                self._func = func
                self._dir = dir

            def __getitem__(self, key):
                # TODO: handle stored key if necessary
                fname = self._key_to_fname(key)
                if exists(fname):
                    file = open(fname, 'r')
                    val = self._func._unpickle(file.read())
                    file.close()
                else:
                    raise KeyError(key)
                return val

            def __setitem__(self, key, val):
                # TODO: handle stored key if necessary
                fname = self._key_to_fname(key)
                file = open(fname, 'w')
                file.write(self._func._pickle(key))
                file.close()

            def __delitem__(self, key):
                fname = self._key_to_fname(key)
                if exists(fname):
                    remove(fname)
                else:
                    raise KeyError(key)

            def __len__(self):
                # TODO: try to filter non-cache files?
                return len(listdir(self._dir))

            def _key_to_fname(self, key):
                h = self._func._hash(key)
                return '%s/%s.out' % (self._dir, h)


        class DiskCacheWithKeys(DiskCache, MutableMapping):
            def __iter__(self):
                return self.KeysIter(self)

            class KeysIter(Iterator):
                def __init__(self, cache):
                    self._cache = cache
                    self._files = listdir(self._cache._dir)
                    self._pos = 0

                def __next__(self):
                    if self._pos >= len(self._files):
                        raise StopIteration
                    fname = join(self._cache._dir, self._files[self._pos])
                    self._pos += 1
                    file = open(fname, 'r')
                    key = self._cache._func._unpickle(file.readline().rstrip('\n'))
                    file.close()
                    return key


    if func is None:
        # @persist(...)
        return persist_wrapper
    else:
        # @persist
        return persist_wrapper(func)


class PersistError(Exception):
    """Exception for errors to do with persistent memoisation"""
