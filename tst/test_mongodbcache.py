import pytest

from pypersist import persist
from pypersist.commoncache import HashCollisionError

import subprocess
from signal import SIGINT
from time import sleep

def test_mongo():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])                                    
    try:
        sleep(1)
        @persist(cache='mongodb://http://127.0.0.1:5000/persist')
        def triple(x):
            return 3 * x
        triple.clear()

        assert len(triple.cache) == 0
        assert triple(3) == 9
        assert len(triple.cache) == 1
        assert triple(3) == 9
        assert len(triple.cache) == 1
        assert triple.cache
        assert triple.cache[(('x', 3),)] == 9
        triple.cache[(('x', 10),)] = 31
        assert triple(10) == 31
        with pytest.raises(KeyError) as ke:
            print(triple.cache[(('x', 5),)])
        assert ke.value.args[0] == (('x', 5),)
        assert len(triple.cache) == 2
        del triple.cache[(('x', 3),)]
        assert len(triple.cache) == 1
        with pytest.raises(KeyError) as ke:
            del triple.cache[(('x', 5),)]
        assert ke.value.args[0] == (('x', 5),)
    finally:
        mongo_process.kill()
