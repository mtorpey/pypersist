from . import hashing
from . import pickling
from . import preprocessing
from . import diskcache
from . import mongodbcache
from functools import update_wrapper


def persist(func=None,
            cache='file://persist/',
            funcname=None,
            key=None,
            storekey=False,
            pickle=pickling.pickle,
            unpickle=pickling.unpickle,
            hash=hashing.hash,
            unhash=None):
    """Function decorator for persistent memoisation

    Store the output of a function permanently, and use previously stored
    results instead of recomputing them.

    To use this, decorate the desired function with `@persist`.
    Or to customise the way this memoisation is done, decorate with
    `@persist(<args>)` and specify custom parameters.

    Parameters
    ----------
    cache : str, optional
        The address of the cache in which the outputs of this function should be
        stored.  If it starts with 'file://', then the remainder of the string
        should be a path to the directory on the local file system in which the
        results will be stored; this may be a relative path, and the directory
        will be created if it does not exist.  If it starts with 'mongodb://'
        then the remainder of the string should be the URL of the pypersist
        MongoDB server in which the results will be stored.  If it does not
        contain '://' then 'file://' will be added at the beginning.  Default is
        'file://persist'.
    funcname : str, optional
        A string that uniquely describes this function.  If the same `cache` is
        used for several memoised functions, they should all have different
        `funcname` values.  Default is the name of the function.
    key : function(args -> object), optional
        Function that takes the arguments given to the memoised function, and
        returns a key that uniquely identifies those arguments.  Two sets of
        arguments should have the same key only if they produce the same output
        when passed into the memoised function.  Default returns a sorted tuple
        describing the arguments along with their names.
    storekey : bool, optional
        Whether to store the key along with the output when a result is stored.
        If True, the key will be checked when recalling a previously computed
        value, to check for hash collisions.  If False, two keys will produce
        the same output whenever their `hash` values are the same.  Default is
        False.
    pickle : function(object -> str), optional
        Function that converts the output of the function to a string for
        storage.  Should be the inverse of `unpickle`.  If `storekey` is true,
        this will also be used to store the key, and should do so without
        newline characters.  Default uses the `pickle` module and base 64
        encoding.
    unpickle : function(str -> object), optional
        Function that converts a string back to an object when retrieving a
        computed value from storage.  Should be the inverse of `pickle`.  If
        `storekey` is true, this will also be used to retrieve the key.
        Default uses the `pickle` module and base 64 encoding.
    hash : function(object -> str), optional
        Function that takes a key and produces a string that will be used to
        identify that key.  If this function is not injective, then `storekey`
        can be set to True to check for hash collisions.  The string should
        only contain characters safe for filenames.  Default uses SHA-256 and
        base 64 encoding, which has an extremely small chance of collision.
    unhash : function(str -> object), optional
        Function that, if specified, should be the inverse of `hash`.  If this
        is specified, it may be used whenever the keys of `cache` are
        requested.  Default is None.

    Attributes
    ----------
    cache : diskcache.Cache or mongodb.Cache
        Dictionary-like object that allows keys to be looked up and, if
        present, gives the previously computed value.  Values can be added and
        removed using the syntax `func.cache[key] = val` and
        `del func.cache[key]`.  If `storekey` is True or `unhash` is specified,
        this implements the collections.abc.MutableMapping abstract base class
        and we can iterate over its keys using `for key in func.cache`.

    Examples
    --------
    Simple persistence using default settings:

    >>> @persist
    ... def double(x):
    ...     return 2 * x
    >>> double(3)
    6
    >>> double(3)
    6
    >>> double.cache[(('x', 3),)]
    6

    Custom persistence using a simpler key, a descriptive filename, and writing
    human-readable files:

    >>> @persist(key=lambda x,y: (x,y),
    ...          hash=lambda k: '%s_to_the_power_of_%s' % k,
    ...          pickle=str,
    ...          unpickle=int)
    ... def power(x, y):
    ...     return x ** y
    >>> power(2,4)
    16
    >>> power(10,3)
    1000
    >>> power.cache[(2,4)]
    16

    """

    class persist_wrapper:

        def __init__(self, func):
            update_wrapper(self, func)
            self._func = func
            self._hash = hash
            self._unhash = unhash
            self._pickle = pickle
            self._unpickle = unpickle
            self._storekey = storekey
            if key is None:
                self._key = self.default_key
            else:
                self._key = key
            if funcname is None:
                self._funcname = func.__name__
            else:
                self._funcname = funcname

            # Determine which backend to use
            try:
                pos = cache.index('://')
                typestring = cache[:pos]
                path = cache[pos+len('://'):]
            except ValueError:
                # No backend specified: use disk
                typestring = 'file'
                path = cache

            cachetypes = {'file': diskcache,
                          'mongodb': mongodbcache}
            cachetype = cachetypes[typestring]

            if storekey or unhash:
                constr = cachetype.CacheWithKeys
            else:
                constr = cachetype.Cache

            self.cache = constr(self, path)

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
