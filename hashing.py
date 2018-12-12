from pickle import dumps
from hashlib import sha1
from base64 import urlsafe_b64encode
CHAR_ENCODING = 'utf-8'


def hash(key):
    """Return a string which is a hash of the argument given.

    It computes the SHA-1 sum of the key and returns it as a base 64 string.
    The string consists of alphanumeric characters, hyphens, and underscores.
    """
    b = dumps(key)  # Pickle the key to a bytes object
    b = sha1(b).digest()  # Hash the bytes using sha-1
    b = urlsafe_b64encode(b)  # Convert the hash to a base64 string
    b = b[0:-1]  # Strip the final padding character ('=')
    s = b.decode(CHAR_ENCODING)  # Convert from bytes to string

    return s
