from os import makedirs, remove, listdir
from os.path import exists, join
from collections.abc import MutableMapping, Iterator


class DiskCache:
    def __init__(self, func, basedir, funcdir=None, storekey=False):
        self._func = func
        self._basedir = basedir
        if funcdir is None:
            self._funcdir = func.__name__
        else:
            self._funcdir = funcdir
        self._dir = join(self._basedir, self._funcdir)
        if not exists(self._dir):
            makedirs(self._dir)
        self._storekey = storekey

    def __getitem__(self, key):
        fname = self._key_to_fname(key)
        if exists(fname):
            file = open(fname, 'r')
            if self._storekey:
                keystring = file.readline().rstrip('\n')
                storedkey = self._func._unpickle(keystring)
                if storedkey == key:
                    print('Key verified')
                else:
                    raise HashCollisionError(storedkey, key)
            val = self._func._unpickle(file.read())
            file.close()
        else:
            raise KeyError(key)
        return val

    def __setitem__(self, key, val):
        fname = self._key_to_fname(key)
        file = open(fname, 'w')
        if self._storekey:
            file.write(self._func._pickle(key))
            file.write('\n')
        file.write(self._func._pickle(val))
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

    def clear(self):
        for f in listdir(self._dir):
            path = join(self._dir, f)
            # TODO: safety checks?
            remove(path)

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
            string = file.readline().rstrip('\n')
            key = self._cache._func._unpickle(string)
            file.close()
            return key


class HashCollisionError(Exception):
    """Exception for when two different keys hash to the same value"""
