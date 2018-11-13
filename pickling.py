from pickle import dumps, loads
from base64 import urlsafe_b64encode, urlsafe_b64decode
CHAR_ENCODING = 'utf-8'


def pickle(obj):
    b = dumps(obj)  # object to bytes
    b64 = urlsafe_b64encode(b)  # bytes to base64 bytes
    s = b64.decode(CHAR_ENCODING)  # base64 bytes to string
    return s


def unpickle(string):
    b64 = string.encode(CHAR_ENCODING)  # string to base64 bytes
    b = urlsafe_b64decode(b64)  # base64 bytes to original bytes
    obj = loads(b)  # bytes to object
    return obj
