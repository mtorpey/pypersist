from . import hashing
from . import pickling
from . import preprocessing
from . import diskcache
from . import mongodbcache
from functools import update_wrapper


def persist(
    func=None,
    cache="file://persist/",
    funcname=None,
    key=None,
    storekey=False,
    pickle=pickling.pickle,
    unpickle=pickling.unpickle,
    hash=hashing.hash,
    unhash=None,
    metadata=None,
):
    """Function decorator for persistent memoisation

    Store the output of a function permanently, and use previously stored
    results instead of recomputing them.

    To use this, decorate the desired function with `@persist`.
    Or to customise the way this memoisation is done, decorate with
    `@persist(<args>)` and specify custom parameters.

    You can even use this decorator for methods in a class.  However, since it
    may be difficult to pickle a class instance, you may wish to specify a
    custom `key` function.

    Parameters
    ----------
    cache : str, optional
        The address of the cache in which the outputs of this function should
        be stored.  If it starts with "file://", then the remainder of the
        string should be a path to the directory on the local file system in
        which the results will be stored; this may be a relative path, and the
        directory will be created if it does not exist.  If it starts with
        "mongodb://" then the remainder of the string should be the URL of the
        pypersist MongoDB server in which the results will be stored.  If it
        does not contain "://" then "file://" will be added at the beginning.
        Default is "file://persist".
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
    metadata : function( -> str), optional
        Function that takes no arguments and returns a string containing
        metadata to be stored with the result currently being written.  This
        might include the current time, or some data identifying the user or
        system that ran the computation.

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
    >>> double.cache[(("x", 3),)]
    6

    Custom persistence using a simpler key, a descriptive filename, and writing
    human-readable files:

    >>> @persist(key=lambda x,y: (x,y),
    ...          hash=lambda k: "%s_to_the_power_of_%s" % k,
    ...          pickle=str,
    ...          unpickle=int)
    ... def power(x, y):
    ...     return x ** y
    >>> power(2,4)
    16
    >>> power(10,3)
    1000
    >>> power.cache[(2, 4)]
    16

    Persistence of a method inside a class.  We specify a key function that
    characterises the relevant parts of the `A` object, since it can be
    difficult to pickle class instances:

    >>> class A:
    ...     def __init__(self, x):
    ...         self.x = x
    ...     @persist(key=lambda self, a: (self.x, a))
    ...     def this_plus_number(self, a):
    ...         return self.x + a
    >>> a = A(5)
    >>> a.this_plus_number(10)
    15
    >>> a.this_plus_number.cache[(5, 10)]
    15
    >>> A.this_plus_number.cache[(5, 10)]
    15

    """

    class persist_wrapper(object):
        def __init__(self, func, instance=None):
            update_wrapper(self, func)
            self._func = func
            self._instance = instance  # supplied iff this is a bound method
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
            self._metadata = metadata

            # Determine which backend to use
            try:
                pos = cache.index("://")
                typestring = cache[:pos]
                path = cache[pos + len("://") :]
            except ValueError:
                # No backend specified: use disk
                typestring = "file"
                path = cache

            cachetypes = {"file": diskcache, "mongodb": mongodbcache}
            cachetype = cachetypes[typestring]

            if storekey or unhash:
                constr = cachetype.CacheWithKeys
            else:
                constr = cachetype.Cache

            self.cache = constr(self, path)

        def __call__(self, *args, **kwargs):
            # Handle "self" argument if this is a class method
            if self._instance is not None:
                args = (self._instance,) + args
            key = self._key(*args, **kwargs)

            # Retrieve or calculate result
            try:
                val = self.cache[key]
            except KeyError:
                val = self._func(*args, **kwargs)
                self.cache[key] = val
            return val

        def __get__(self, instance, cls):
            """Bind this object to an instance of a class, as a method

            If `func` was defined as a method of a class, then `__get__` will
            be called by Python when an instance of that class calls the method
            for the first time.  We create a new `persist_wrapper` object in
            exactly the same way this one was created, but supplying an
            `instance` argument, which is the class instance to which this
            method should be bound.

            We replace the instance's method (this wrapper) with the new
            wrapper, and then return it.  In this way, we ensure that this
            `__get__` function is only called once per instance.  The `__get__`
            function of the new wrapper should never be called.

            If `func` was not defined as a method of a class, this will
            probably never be called.  But if it is, then the present wrapper
            is returned unchanged.

            Parameters
            ----------
            self : persist_wrapper
                This wrapper, i.e. the memoised method that is being called.
                It should not yet have been called with the instance to which
                it is now bound.
            instance : cls
                An instance of the class in which this function was defined as
                a method.  This instance is currently trying to use the method
                for the first time, and so requires a version of the method
                that is bound to it.
            cls : class
                The class in which this function was defined as a method.
                `instance` is an instance of this class.

            Returns
            -------
            persist_wrapper
                A new wrapper for the same function as `self`, but bound to the
                instance.  If the instance is `None` (unlikely) then `self` is
                returned.

            """

            if instance is None:
                return self
            bound_method_wrapper = persist_wrapper(self._func, instance)
            setattr(instance, self._func.__name__, bound_method_wrapper)
            return bound_method_wrapper

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
