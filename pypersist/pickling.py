"""Default methods used by `persist` for pickling and unpickling objects."""

from pickle import dumps, loads
from base64 import urlsafe_b64encode, urlsafe_b64decode
from sys import modules

CHAR_ENCODING = "utf-8"


def pickle(obj):
    """Return a string representation of the object `obj`

    This function takes any object, and uses the `pickle` and `base64` modules
    to create a string which represents it.  This string consists only of
    alphanumeric characters, hyphens and underscores.  The object `obj` can
    later be reconstructed from this string using the `unpickle` function.

    Examples
    --------
    >>> pickle("Hello world")
    'gANYCwAAAEhlbGxvIHdvcmxkcQAu'
    >>> unpickle("gANYCwAAAEhlbGxvIHdvcmxkcQAu")
    'Hello world'

    """
    b = pickle_to_bytes(obj)  # object to bytes
    b64 = urlsafe_b64encode(b)  # bytes to base64 bytes
    s = b64.decode(CHAR_ENCODING)  # base64 bytes to string
    return s


def unpickle(string):
    """Restore an object from a string created by the `pickle` function

    If `string` was created by the `pickle` function in this file, then this
    function returns an object identical to the one that was used to create
    `string`.

    Examples
    --------
    >>> pickle("Hello world")
    'gANYCwAAAEhlbGxvIHdvcmxkcQAu'
    >>> unpickle("gANYCwAAAEhlbGxvIHdvcmxkcQAu")
    'Hello world'

    """
    b64 = string.encode(CHAR_ENCODING)  # string to base64 bytes
    b = urlsafe_b64decode(b64)  # base64 bytes to original bytes
    obj = unpickle_from_bytes(b)  # bytes to object
    return obj


def pickle_to_bytes(obj):
    """Pickle an object to a bytes object

    For most objects, this function is equivalent to `pickle.dumps`.  However,
    if `pickle.dumps` fails, then an alternative pickling method will be
    attempted using Sage, if Sage is loaded.  Otherwise, an error will be
    raised.

    Used inside the `pickle` function in this file.

    """
    try:
        b = dumps(obj)  # Pickle the key to a bytes object
    except Exception as e:  # Should be a PickleError, but doesn't seem to be
        if "sage.misc.persist" in modules:  # Use Sage pickling if necessary
            import sage.misc.persist

            b = sage.misc.persist.dumps(obj)
        else:
            raise e  # Still can't pickle - raise error
    return b


def unpickle_from_bytes(obj):
    """Unpickle a bytes object to produce the original object that was pickled

    For most objects, this function is equivalent to `pickle.loads`.  However,
    if `pickle.loads` fails, then an alternative unpickling method will be
    attempted using Sage, if Sage is loaded.  Otherwise, an error will be
    raised.

    Used inside the `unpickle` function in this file.

    """
    try:
        b = loads(obj)  # Pickle the key to a bytes object
    except Exception as e:  # Should be a PickleError, but doesn't seem to be
        if "sage.misc.persist" in modules:  # Use Sage unpickling if necessary
            import sage.misc.persist

            b = sage.misc.persist.loads(obj)
        else:
            raise e  # Still can't pickle - raise error
    return b
