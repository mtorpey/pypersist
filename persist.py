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
    """Function decorator for persistent memoisation

    Store the output of a function permanently, and use previously stored
    results instead of recomputing them.

    To use this, decorate the desired function with `@persist`.
    Or to customise the way this memoisation is done, decorate with
    `@persist(<args>)` and specify custom parameters.

    Parameters
    ----------
    basedir : str, optional
        The directory in which results should be stored, possibly along with
        results from other functions.  Default is 'persist'.
    funcdir : str, optional
        The directory inside `basedir` in which the results for just this
        function should be stored.  Default is the name of the function.
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
    pickle : function(object -> string), optional
        Function that converts the output of the function to a string for
        storage.  Should be the inverse of `unpickle`.  If `storekey` is true,
        this will also be used to store the key, and should do so without
        newline characters.  Default uses the `pickle` module and base 64
        encoding.
    unpickle : function(string -> object), optional
        Function that converts a string back to an object when retrieving a
        computed value from storage.  Should be the inverse of `pickle`.  If
        `storekey` is true, this will also be used to retrieve the key.
        Default uses the `pickle` module and base 64 encoding.
    hash : function(object -> string), optional
        Function that takes a key and produces a string that will be used to
        identify that key.  If this function is not injective, then `storekey`
        should be set to True to check for hash collisions.  The string should
        only contain characters safe for filenames.  Default uses SHA-1 and
        base 64 encoding, which can store 10^22 objects with a <0.01% chance of
        a collision.

    Attributes
    ----------
    cache : diskcache.DiskCache
        Dictionary-like object that allows keys to be looked up and, if
        present, gives the previously computed value.  Values can be added and
        removed using the syntax `func.cache[key] = val` and
        `del func.cache[key]`.  If `storekey` is true, this implements the
        collections.abc.MutableMapping abstract base class and we can iterate
        over its keys using `for key in func.cache`.

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
            self._pickle = pickle
            self._unpickle = unpickle
            if key is None:
                self._key = self.default_key
            else:
                self._key = key
            if storekey:
                # can iterate over keys
                constr = diskcache.DiskCacheWithKeys
            else:
                # cannot iterate over keys
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
