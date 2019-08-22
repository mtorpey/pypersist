"""Persistent memoisation backend that saves results in the local file system

The `persist` decorator takes a `cache` argument, which details what sort of
backend to use for the cache.  If this string begins with "file://", or if no
`cache` is specified, then a *disk cache* is used, which saves computed results
to a directory in the local file system.  This internal work is done by the
classes defined below.

"""

from .commoncache import HashCollisionError

from collections import MutableMapping, Iterator
from os import makedirs, remove, listdir
from os.path import exists, join
from time import sleep


class Cache:
    """Dictionary-like object for saving function outputs to disk

    This cache, which can be used by the `persist` decorator in `persist.py`,
    stores computed values on disk in a specified directory so that they can be
    restored later using a key.  Like a dictionary, a key-value pair can be
    added using `cache[key] = val`, looked up using `cache[key]`, and removed
    using `del cache[key]`.  The number of values stored can be found using
    `len(cache)`.

    A disk cache might not store its keys, and therefore we cannot iterate
    through its keys as we can with a dictionary.  However, see
    `CacheWithKeys`.

    Parameters
    ----------
    func : persist_wrapper
        Memoised function whose results this is caching.  Options which are not
        specific to local disk storage, such as the key, hash, and pickle
        functions, are taken from this.
    dir : str
        Directory into which to save results.  The same directory can be used
        for several different functions, since a subdirectory will be created
        for each function based on its `funcname`.

    """

    def __init__(self, func, dir):
        self._func = func
        self._dir = join(dir, self._func._funcname)
        if not exists(self._dir):
            makedirs(self._dir)

    def __getitem__(self, key):
        lockfname = self._key_to_fname(key, LOCK)
        while exists(lockfname):
            sleep(0.1)  # wait before reading
        fname = self._key_to_fname(key, OUT)
        if self._func._unhash:
            storedkey = self._fname_to_key(fname)
            if storedkey != key:
                raise HashCollisionError(storedkey, key)
        if exists(fname):
            if self._func._storekey:
                keyfname = self._key_to_fname(key, KEY)
                assert exists(fname)
                keyfile = open(keyfname, "r")
                keystring = keyfile.read()
                keyfile.close()
                storedkey = self._func._unpickle(keystring)
                if storedkey != key:
                    raise HashCollisionError(storedkey, key)
            file = open(fname, "r")
            val = self._func._unpickle(file.read())
            file.close()
        else:
            raise KeyError(key)
        return val

    def __setitem__(self, key, val):
        to_write = []  # list of (filename, string) pairs

        # .out file
        outfname = self._key_to_fname(key, OUT)
        outstring = self._func._pickle(val)
        to_write.append((outfname, outstring))

        # .key file
        if self._func._storekey:
            keyfname = self._key_to_fname(key, KEY)
            keystring = self._func._pickle(key)
            to_write.append((keyfname, keystring))

        # .meta file
        if self._func._metadata:
            metafname = self._key_to_fname(key, META)
            metastring = self._func._metadata()
            to_write.append((metafname, metastring))

        # get a lock on this result
        lockfname = self._key_to_fname(key, LOCK)
        if exists(lockfname) or exists(outfname):
            return  # another thread got here first - abort!
        open(lockfname, "w").close()

        # do the file operations
        print(to_write)
        for (fname, string) in to_write:
            file = open(fname, "w")
            file.write(string)
            file.close()

        # unlock this result
        remove(lockfname)

    def __delitem__(self, key):
        lockfname = self._key_to_fname(key, LOCK)
        while exists(lockfname):
            sleep(0.1)  # wait before reading
        for ext in [OUT, KEY, META]:
            fname = self._key_to_fname(key, ext)
            if exists(fname):
                remove(fname)
            elif ext == OUT:
                raise KeyError(key)

    def __len__(self):
        # Number of files ending with ".out"
        return sum(fname.endswith(OUT) for fname in listdir(self._dir))

    def clear(self):
        """Delete all the results stored in this cache"""
        for f in listdir(self._dir):
            path = join(self._dir, f)
            # TODO: safety checks?
            remove(path)

    def _key_to_fname(self, key, ext):
        h = self._func._hash(key)
        return join(self._dir, h + ext)

    def _fname_to_key(self, fname):
        if fname.startswith(self._dir):
            fname = fname[len(self._dir + "/") :]  # remove directory
        h = fname[: fname.rfind(".")]  # remove extension
        return self._func._unhash(h)


class CacheWithKeys(Cache, MutableMapping):
    """Mutable mapping for saving function outputs to disk

    This subclass of `Cache` can be used in place of `Cache` whenever
    `storekey` is True or `unhash` is set, to implement the `MutableMapping`
    abstract base class.  This allows the cache to be used exactly like a
    dictionary, including the ability to iterate through all keys in the cache.

    """

    def __iter__(self):
        return self.KeysIter(self)

    class KeysIter(Iterator):
        """Iterator class for the keys of a `CacheWithKeys` object"""

        def __init__(self, cache):
            self._cache = cache
            self._pos = 0
            self._files = [
                fname
                for fname in listdir(self._cache._dir)
                if fname.endswith(OUT)
            ]

        def __next__(self):
            if self._pos >= len(self._files):
                raise StopIteration
            fname = self._files[self._pos]
            if self._cache._func._unhash:
                # Unhash from filename
                key = self._cache._fname_to_key(fname)
            else:
                assert self._cache._func._storekey
                # Read key from file
                path = join(self._cache._dir, fname)
                path = path[: -len(OUT)] + KEY
                file = open(path, "r")
                string = file.read()
                key = self._cache._func._unpickle(string)
                file.close()
            self._pos += 1
            return key

        next = __next__  # for Python 2 compatibility


# Filename extensions
OUT = ".out"
KEY = ".key"
META = ".meta"
LOCK = ".lock"
