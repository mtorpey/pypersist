from time import sleep
import pickling
import preprocessing
from functools import update_wrapper
from os import makedirs
from os.path import exists


def persist(func=None,
            dir='persist',
            pickle=pickling.pickle,
            unpickle=pickling.unpickle,
            format_args=None):

    class persist_wrapper:

        def __init__(self, func):
            update_wrapper(self, func)
            self._func = func
            self._hash = hash
            self._dir = dir + '/' + func.__name__
            if not exists(self._dir):
                makedirs(self._dir)
            self._pickle = pickle
            self._unpickle = unpickle

        def __call__(self, *args, **kwargs):
            key = self.key(*args, **kwargs)
            h = self._hash(key)
            fname = self.filename(h)
            if exists(fname):
                print(f'''Retrieving cached value for {key} (in {dir}). . .''')
                file = open(fname, 'r')
                val = self._unpickle(file.read())
                file.close()
            else:
                print(f'''Computing value for {key} . . .''')
                val = self._func(*args, **kwargs)
                print(f'''Writing to {fname} . . .''')
                file = open(fname, 'w')
                file.write(self._pickle(val))
                file.close()
            return val

        def key(self, *args, **kwargs):
            k = preprocessing.arg_tuple(self._func, *args, **kwargs)
            return k

        def filename(self, h):
            return '%s/_%s.out' % (self._dir, h)

    if func is None:
        # @persist(...)
        return persist_wrapper
    else:
        # @persist
        return persist_wrapper(func)


@persist(dir='dirfortriple')
def triple(x):
    sleep(1)
    return 3 * x


@persist(pickle=repr, unpickle=eval)
def double(x):
    sleep(1)
    return 2 * x


@persist
def foo(x, y, z=1, a=3):
    print(x, y, z, a)
    return x + y + z + a
