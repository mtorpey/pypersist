"""Persistent memoisation backend that saves results on a MongoDB REST server

The `persist` decorator takes a `cache` argument, which details what sort of
backend to use for the cache.  If this string begins with "mongodb://", then a
*MongoDB cache* is used, which saves computed results to a MongoDB database via
a REST API.  This internal work is done by the classes defined below.

To start a MongoDB/REST server for use with this cache, navigate to the
`mongodb_server/` directory and execute the `run.py` script.

"""

from .commoncache import HashCollisionError

from collections import MutableMapping, Iterator
from os.path import join
import requests
import json


class Cache:
    """Dictionary-like object for saving function outputs to disk

    This cache, which can be used by the `persist` decorator in `persist.py`,
    stores computed values in a specified MongoDB database so that they can be
    restored later using a key.  Like a dictionary, a key-value pair can be
    added using `cache[key] = val`, looked up using `cache[key]`, and removed
    using `del cache[key]`.  The number of values stored can be found using
    `len(cache)`.

    A MongoDB cache might not store its keys, and therefore we cannot iterate
    through its keys as we can with a dictionary.  However, see
    `CacheWithKeys`.

    Parameters
    ----------
    func : persist_wrapper
        Memoised function whose results this is caching.  Options which are not
        specific to local disk storage, such as the key, hash, and pickle
        functions, are taken from this.
    url : str
        URL of the pypersist MongoDB database that will be used to store and
        load results.  The same database can be used for several different
        functions, since the function's `funcname` will be stored with each
        result.

    """

    def __init__(self, func, url):
        self._func = func

        # Use http if not specified
        if url.find("://") == -1:
            url = "http://" + url
        self._url = join(url, self._func._funcname)
        self._headers = {
            "Content-type": "application/json",
            "Accept": "text/plain",
        }

    def __getitem__(self, key):
        # Get hash and check it
        h = self._func._hash(key)
        if self._func._unhash:
            storedkey = self._func._unhash(h)
            if storedkey != key:
                raise HashCollisionError(storedkey, key)

        # Search for value in database
        db_item = self._get_db(h)
        if db_item:
            # Stored value found
            if self._func._storekey:
                # Check key
                keystring = db_item["key"]
                storedkey = self._func._unpickle(keystring)
                if storedkey != key:
                    raise HashCollisionError(storedkey, key)
            # Use stored value
            val = self._func._unpickle(db_item["result"])
        else:
            # No value stored
            raise KeyError(key)

        return val

    def __setitem__(self, key, val):
        h = self._func._hash(key)
        new_item = {
            "funcname": self._func._funcname,
            "hash": h,
            "namespace": "pypersist",  # TODO: make configurable
            "result": self._func._pickle(val),
        }
        if self._func._storekey:
            new_item["key"] = self._func._pickle(key)
        if self._func._metadata:
            new_item["metadata"] = self._func._metadata()

        r = requests.post(url=self._url, headers=self._headers, json=new_item)
        r.raise_for_status()

    def __delitem__(self, key):
        # Get the item from the database
        h = self._func._hash(key)
        db_item = self._get_db(h)
        if db_item is None:
            raise KeyError(key)

        # Delete the item using its _id and _etag
        url = self._url + "/" + db_item["_id"]
        headers = dict(self._headers)
        headers["If-Match"] = db_item["_etag"]
        r = requests.delete(url=url, headers=headers)
        r.raise_for_status()

    def __len__(self):
        db_items = self._get_db()
        if db_items:
            return db_items["_meta"]["total"]
        else:
            return 0

    def clear(self):
        """Delete all the results stored in this cache"""
        r = requests.delete(url=self._url)
        if r.status_code not in [204, 404]:
            r.raise_for_status()

    def _get_db(self, hash=None):
        """Return all db items for this function, or one with this hash

        Queries the MongoDB database for entries with this function, and
        returns the resulting json data as a dictionary.

        Parameters
        ----------
        hash : str, optional
          The hash of the database item we wish to retrieve.

        Returns
        -------
        dict or None
          If a hash is specified, a single database item with entries "_id",
          "_etag", "funcname", "hash", "result" and so on.  If no hash is
          specified, a list of all such items in the database in the "_items"
          entry, along with metadata in the "_meta" entry.
          If no appropriate item exists in the database, None.

        """
        url = self._url
        if hash:
            url += "/" + hash
        r = requests.get(url=url)
        if r.status_code == 200:
            # Stored values found
            return json.loads(r.text)
        elif r.status_code == 404:
            # No value stored
            return None
        else:
            # Database error
            r.raise_for_status()


class CacheWithKeys(Cache, MutableMapping):
    """Mutable mapping for saving function outputs to a MongoDB database

    This subclass of `Cache` can be used in place of `Cache` whenever
    `storekey` is True or `unhash` is defined, to implement the
    `MutableMapping` abstract base class.  This allows the cache to be used
    exactly like a dictionary, including the ability to iterate through all
    keys in the cache.

    """

    def __iter__(self):
        return self.KeysIter(self)

    class KeysIter(Iterator):
        """Iterator class for the keys of a `CacheWithKeys` object"""

        def __init__(self, cache):
            self._cache = cache
            assert cache._func._storekey or cache._func._unhash
            db_items = self._cache._get_db()
            if db_items:
                self._items = db_items["_items"]
            else:
                self._items = []
            self._pos = 0

        def __next__(self):
            if self._pos >= len(self._items):
                raise StopIteration
            item = self._items[self._pos]
            self._pos += 1
            if self._cache._func._storekey:
                key = self._cache._func._unpickle(item["key"])
            else:
                assert self._cache._func._unhash
                key = self._cache._func._unhash(item["hash"])
            return key

        next = __next__  # for Python 2 compatibility
