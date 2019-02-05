"""Default methods used by `persist` for pickling and unpickling objects."""

from pickle import dumps, loads
from base64 import urlsafe_b64encode, urlsafe_b64decode
CHAR_ENCODING = 'utf-8'


def pickle(obj):
    """Return a string representation of the object `obj`

    This function takes any object, and uses the `pickle` and `base64` modules
    to create a string which represents it.  This string consists only of
    alphanumeric characters, hyphens and underscores.  The object `obj` can
    later be reconstructed from this string using the `unpickle` function.

    Examples
    --------
    >>> pickle('Hello world')
    'gANYCwAAAEhlbGxvIHdvcmxkcQAu'
    >>> unpickle('gANYCwAAAEhlbGxvIHdvcmxkcQAu')
    'Hello world'

    """
    b = dumps(obj)  # object to bytes
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
    >>> pickle('Hello world')
    'gANYCwAAAEhlbGxvIHdvcmxkcQAu'
    >>> unpickle('gANYCwAAAEhlbGxvIHdvcmxkcQAu')
    'Hello world'

    """
    b64 = string.encode(CHAR_ENCODING)  # string to base64 bytes
    b = urlsafe_b64decode(b64)  # base64 bytes to original bytes
    obj = loads(b)  # bytes to object
    return obj
