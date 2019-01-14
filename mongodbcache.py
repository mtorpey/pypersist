from collections.abc import MutableMapping, Iterator
import requests, json


class MongoDBCache:
    """Dictionary-like object for saving function outputs to disk

    This cache, which can be used by the `persist` decorator in `persist.py`,
    stores computed values to disk in a specified directory so that they can be
    restored later using a key.  Like a dictionary, a key-value pair can be
    added using `cache[key] = val`, looked up using `cache[key]`, and removed
    using `del cache[key]`.  The number of values stored can be found using
    `len(cache)`.

    A MongoDBCache might not store its keys, and therefore we cannot iterate
    through its keys as we can with a dictionary.  However, see
    `MongoDBCacheWithKeys`.

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
        using the subclass `MongoDBCacheWithKeys`.  Default is False.

    """

    def __init__(self, func, funcname=None, storekey=False):
        self._func = func
        if funcname is None:
            self._funcname = func.__name__
        else:
            self._funcname = funcname
        self._storekey = storekey
        self._url = 'http://localhost:5000/memos/' + self._funcname
        self._headers = {'Content-type': 'application/json',
                         'Accept': 'text/plain'}

    def __getitem__(self, key):
        # Get hash and check it
        h = self._func._hash(key)
        if self._func._unhash:
            storedkey = self._func._unhash(h)
            if storedkey != key:
                raise HashCollisionError(storedkey, key)

        # Search for hash in database
        url = self._url + '/' + h
        r = requests.get(url=url)
        if r.status_code == 200:
            # Stored value found
            db_item = json.loads(r.text)
            if self._storekey:
                # Check key
                keystring = db_item.get('key')
                storedkey = self._func._unpickle(keystring)
                if storedkey != key:
                    raise HashCollisionError(storedkey, key)
            # Use stored value
            val = self._func._unpickle(db_item.get('result'))
        elif r.status_code == 404:
            # No value stored
            raise KeyError(key)
        else:
            # Database error
            raise MongoDBError(r.status_code, r.reason)

        return val

    def __setitem__(self, key, val):
        h = self._func._hash(key)
        new_item = {'funcname': self._funcname,
                    'hash': h,
                    'namespace': 'pymemo',
                    'result': self._func._pickle(val)}
        if self._storekey:
            new_item['key'] = self._func._pickle(key)
        r = requests.post(url=self._url,
                          headers=self._headers,
                          json=new_item)
        if r.status_code != 201:
            raise MongoDBError(r.status_code, r.reason)

    def __delitem__(self, key):
        # Get the item from the database
        h = self._func._hash(key)
        url = self._url + '/' + h
        r = requests.get(url=url)
        if r.status_code == 200:
            db_item = json.loads(r.text)
        elif r.status_code == 404:
            raise KeyError(key)
        else:
            raise MongoDBError(r.status_code, r.reason)

        # Delete the item using its _id and _etag
        url = self._url + '/' + db_item.get('_id')
        headers = dict(self._headers)
        headers['If-Match'] = db_item.get('_etag')
        r = requests.delete(url=url, headers=headers)
        if r.status_code != 204:
            raise MongoDBError(r.status_code, r.reason)

    def __len__(self):
        r = requests.get(url=self._url)
        if r.status_code == 200:
            return json.loads(r.text).get('_meta').get('total')
        elif r.status_code == 404:
            return 0
        else:
            raise MongoDBError(r.status_code, r.reason)

    def clear(self):
        """Delete all the results stored in this cache."""
        r = requests.delete(url=self._url)
        if r.status_code not in [204, 404]:
            raise MongoDBError(r.status_code, r.reason)


class MongoDBCacheWithKeys(MongoDBCache, MutableMapping):
    """Mutable mapping for saving function outputs to a MongoDB database

    This subclass of `MongoDBCache` can be used in place of `MongoDBCache`
    whenever `storekey` is True, to implement the `MutableMapping` abstract base
    class.  This allows the cache to be used exactly like a dictionary,
    including the ability to iterate through all keys in the cache.

    """

    def __iter__(self):
        return self.KeysIter(self)

    class KeysIter(Iterator):
        """Iterator class for the keys of a `MongoDBCacheWithKeys` object"""
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


class MongoDBCacheWithUnhash(MongoDBCache, MutableMapping):
    """Mutable mapping for saving function outputs to a MongoDB database

    This subclass of `MongoDBCache` can be used in place of `MongoDBCache`
    whenever `unhash` is set, to implement the `MutableMapping` abstract base
    class.  This allows the cache to be used exactly like a dictionary,
    including the ability to iterate through all keys in the cache by unhashing
    filenames.

    """

    def __iter__(self):
        return self.KeysIter(self)

    class KeysIter(Iterator):
        """Iterator class for the keys of a `MongoDBCacheWithUnhash` object"""
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

class MongoDBError(Exception):
    """Exception for a problem interacting with a MongoDB database"""
