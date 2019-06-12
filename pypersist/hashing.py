"""Default method used by `persist` for hashing keys"""

from . import pickling
from hashlib import sha256
from base64 import urlsafe_b64encode

CHAR_ENCODING = "utf-8"


def hash(key):
    """Return a string which is a hash of the argument given

    It computes the SHA-256 sum of the key and returns it as a base 64 string.
    The string consists of alphanumeric characters, hyphens and underscores,
    and is precisely 43 characters long.

    Examples
    --------
    >>> hash("somestringkey123")
    'wXS1bv_UbdX4riiyyA3Djjo7JeiEfyGI7o1-hGMnkz0'
    >>> hash(3.141592654)
    'nAh_dG9CDZL7bAFWX7E3iUXN2HXZ5eUiYUzdCJXDH-k'
    >>> hash(None)
    'Tz_DSKgYlBpGTkFf_2udQWwd3DscZHQ4YdMo-8NFvNY'
    >>> key = (("arg1", [1,1,2,3,5,8,13]), ("x", "hello"))
    >>> hash(key)
    '1TBQNjqeAKCcCBmy-Sk_T1Xm01juuHOWiKotF5WYeZ8'
    >>> hash("somestringkey123")
    'wXS1bv_UbdX4riiyyA3Djjo7JeiEfyGI7o1-hGMnkz0'

    """
    b = pickling.pickle_to_bytes(key)  # Pickle the key to a bytes object
    b = sha256(b).digest()  # Hash the bytes using sha-256
    b = urlsafe_b64encode(b)  # Convert the hash to a base64 string
    assert b.endswith(b"=")
    b = b[0:-1]  # Strip the final padding character ("=")
    s = b.decode(CHAR_ENCODING)  # Convert from bytes to string

    return s
