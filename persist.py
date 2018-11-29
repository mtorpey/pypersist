import hashing
import pickling
import preprocessing
from functools import update_wrapper
from os import makedirs, remove, listdir
from os.path import exists, join
from re import compile


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

    if func is None:
        # @persist(...)
        return persist_wrapper
    else:
        # @persist
        return persist_wrapper(func)


class PersistError(Exception):
    """Exception for errors to do with persistent memoisation"""
