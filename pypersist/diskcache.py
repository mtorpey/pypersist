"""Persistent memoisation backend that saves results in the local file system

The `persist` decorator takes a `cache` argument, which details what sort of
backend to use for the cache.  If this string begins with "file://", or if no
`cache` is specified, then a *disk cache* is used, which saves computed results
to a directory in the local file system.  This internal work is done by the
classes defined below.

"""

from .commoncache import HashCollisionError

from os import makedirs, remove, listdir
from os.path import exists, join
from time import sleep

from sys import version_info

PYTHON_VERSION = version_info[0]  # major version number
if PYTHON_VERSION >= 3:
    from collections.abc import MutableMapping, Iterator
else:
    from collections import MutableMapping, Iterator


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

        # print that we are getting cache depending on verbosity.
        if self._func._verbosity == 3:
            print("Getting cache.")
        elif self._func._verbosity > 3:
            print(
                "Getting key {key} from {fname}.".format(key=key, fname=fname)
            )

        if self._func._unhash:
            storedkey = self._fname_to_key(fname)
            if storedkey != key:
                # Print errors if high enough verbosity.
                if self._func._verbosity > 0:
                    print(
                        "Key ({key}) does not match stored key ({storedkey}).".format(
                            key=key, storedkey=storedkey
                        )
                    )
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
            # Not found in cache (not a problem, just a new value)
            if self._func._verbosity >= 3:
                print(
                    "No entry for {key} as {fname} does not exist.".format(
                        key=key, fname=fname
                    )
                )
            raise KeyError(key)

        # print that we are done getting cache.
        if self._func._verbosity > 2:
            print("Done reading cache.")

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

        # print what we're writing with different levels of verbosity
        if self._func._verbosity == 2:
            print("Writing to files.")

        # do the file operations
        for (fname, string) in to_write:
            if self._func._verbosity == 3:
                print("Writing to {fname}".format(fname=fname))
            elif self._func._verbosity > 3:
                print(
                    "Writing {string} to {fname}".format(
                        string=string, fname=fname
                    )
                )
            file = open(fname, "w")
            file.write(string)
            file.close()
            if self._func._verbosity > 2:
                print("Done writing {fname}.".format(fname=fname))

        # print that files have been written
        if self._func._verbosity > 2:
            print("Done writing all files.")

        # unlock this result
        remove(lockfname)

    def __delitem__(self, key):
        lockfname = self._key_to_fname(key, LOCK)
        while exists(lockfname):
            sleep(0.1)  # wait before reading

        # print what we're deleting depending on verbosity.
        if self._func._verbosity == 3:
            print("Deleting cache item.")

        for ext in [OUT, KEY, META]:
            fname = self._key_to_fname(key, ext)

            # print what we're deleting depending on verbosity.
            if self._func._verbosity > 3:
                print(
                    "Deleting cache item {key} in file {fname}.".format(
                        fname=fname, key=key
                    )
                )

            if exists(fname):
                remove(fname)
            elif ext == OUT:
                raise KeyError(key)

            # print what we've finished deleting
            if self._func._verbosity > 3:
                print("File {fname} deleted.".format(fname=fname))

        # print that we're done deleting
        if self._func._verbosity >= 3:
            print("Done deleting cache item.")

    def __len__(self):
        # Number of files ending with ".out"
        return sum(fname.endswith(OUT) for fname in listdir(self._dir))

    def clear(self):
        """Delete all the results stored in this cache"""

        # print that we are clearing cache depending on verbosity.
        if self._func._verbosity > 1:
            print("Clearing cache.")

        for f in listdir(self._dir):
            path = join(self._dir, f)
            # TODO: safety checks?
            remove(path)

        # print that we are done clearing cache depending on verbosity.
        if self._func._verbosity > 2:
            print("Cache cleared.")

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
