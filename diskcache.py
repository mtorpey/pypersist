from os import makedirs, remove, listdir
from os.path import exists, join
from collections.abc import MutableMapping, Iterator


class DiskCache:
    """Dictionary-like object for saving function outputs to disk

    This cache, which can be used by the `persist` decorator in `persist.py`,
    stores computed values to disk in a specified directory so that they can be
    restored later using a key.  Like a dictionary, a key-value pair can be
    added using `cache[key] = val`, looked up using `cache[key]`, and removed
    using `del cache[key]`.  The number of values stored can be found using
    `len(cache)`.

    A DiskCache might not store its keys, and therefore we cannot iterate
    through its keys as we can with a dictionary.  However, see
    `DiskCacheWithKeys`.

    Parameters
    ----------
    func : persist_wrapper
        Memoised function whose results this is caching.  Options which are not
        specific to local disk storage, such as the key, hash, and pickle
        functions, are taken from this.
    basedir : str
        Directory into which to save results.  The same directory can be used
        for several different functions.
    funcdir : str, optional
        Directory inside `basedir` into which the results for this specific
        function should be stored.  Should be unique to avoid returning results
        for the wrong function.  Default is the name of the function `func`.
    storekey : bool, optional
        Whether to store the key along with the output when a result is stored.
        If True, the key will be checked when loading a value, to check for
        hash collisions.  If False, two keys will produce the same output
        whenever their `hash` values are the same.  If True is used, consider
        using the subclass `DiskCacheWithKeys`.  Default is False.

    """

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
        if self._func._unhash:
            storedkey = self._fname_to_key(fname)
            if storedkey != key:
                raise HashCollisionError(storedkey, key)
        if exists(fname):
            file = open(fname, 'r')
            if self._storekey:
                keystring = file.readline().rstrip('\n')
                storedkey = self._func._unpickle(keystring)
                if storedkey != key:
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
        """Delete all the results stored in this cache."""
        for f in listdir(self._dir):
            path = join(self._dir, f)
            # TODO: safety checks?
            remove(path)

    def _key_to_fname(self, key):
        h = self._func._hash(key)
        return '%s/%s.out' % (self._dir, h)

    def _fname_to_key(self, fname):
        if fname.startswith(self._dir):
            fname = fname[len(self._dir + '/'):]  # remove directory
        h = fname[:-len('.out')]  # remove '.out'
        return self._func._unhash(h)


class DiskCacheWithKeys(DiskCache, MutableMapping):
    """Mutable mapping for saving function outputs to disk

    This subclass of `DiskCache` can be used in place of `DiskCache` whenever
    `storekeys` is True, to implement the `MutableMapping` abstract base class.
    This allows the cache to be used exactly like a dictionary, including the
    ability to iterate through all keys in the cache.

    """

    def __iter__(self):
        return self.KeysIter(self)

    class KeysIter(Iterator):
        """Iterator class for the keys of a `DiskCacheWithKeys` object"""
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


class DiskCacheWithUnhash(DiskCache, MutableMapping):
    """Mutable mapping for saving function outputs to disk

    This subclass of `DiskCache` can be used in place of `DiskCache` whenever
    `unhash` is set, to implement the `MutableMapping` abstract base class.
    This allows the cache to be used exactly like a dictionary, including the
    ability to iterate through all keys in the cache by unhashing filenames.

    """

    def __iter__(self):
        return self.KeysIter(self)

    class KeysIter(Iterator):
        """Iterator class for the keys of a `DiskCacheWithUnhash` object"""
        def __init__(self, cache):
            self._cache = cache
            self._files = listdir(self._cache._dir)
            self._pos = 0

        def __next__(self):
            if self._pos >= len(self._files):
                raise StopIteration
            fname = self._files[self._pos]
            h = fname[:-len('.out')]  # cut off '.out'
            self._pos += 1
            return self._cache._func._unhash(h)


class HashCollisionError(Exception):
    """Exception for when two different keys hash to the same value"""
