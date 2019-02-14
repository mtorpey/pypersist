import pytest

from pypersist import persist
from pypersist.commoncache import HashCollisionError

import subprocess
from requests import ConnectionError
from time import sleep

SLEEP_TIME = 0.5

def test_mongo():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])                                    
    sleep(SLEEP_TIME)
    try:
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

def test_hash():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])
    sleep(SLEEP_TIME)
    try:
        @persist(hash=lambda k: '%s to the %s' % (k[0][1], k[1][1]),
                 pickle=str,
                 unpickle=int,
                 cache='mongodb://127.0.0.1:5000/persist')
        def pow(x,y):
            return x**y
        pow.clear()

        assert pow(2,3) == 8
        assert pow(7,4) == 2401
        assert pow(1,3) == 1
        assert pow(10,5) == 100000
        assert pow(0,0) == 1
        assert pow(2,16) == 65536
        assert pow(10,5) == 100000
        assert pow(0,0) == 1

        assert len(pow.cache) == 6
    finally:
        mongo_process.kill()

def test_storekey():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])
    sleep(SLEEP_TIME)
    try:
        @persist(storekey=True,
                 cache='mongodb://127.0.0.1:5000/persist')
        def square(x):
            return x*x
        square.clear()

        items = [key for key in square.cache.items()]
        assert sorted(items) == []

        assert square(12) == 144
        assert square(0) == 0
        assert square(8) == 64

        keys = [key for key in square.cache]
        assert sorted(keys) == [(('x', 0),), (('x', 8),), (('x', 12),)]
        keys = [key for key in square.cache.keys()]
        assert sorted(keys) == [(('x', 0),), (('x', 8),), (('x', 12),)]
        values = [key for key in square.cache.values()]
        assert sorted(values) == [0, 64, 144]
        items = [key for key in square.cache.items()]
        assert sorted(items) == [((('x', 0),), 0),
                                 ((('x', 8),), 64),
                                 ((('x', 12),), 144)]
    finally:
        mongo_process.kill()

def test_hash_collision():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])
    sleep(SLEEP_TIME)
    try:
        @persist(hash=lambda k: 'hello world',
                 cache='mongodb://127.0.0.1:5000/persist')
        def square(x):
            return x*x
        square.clear()
        assert square(3) == 9
        assert square(4) == 9

        @persist(hash=lambda k: 'hello world', storekey=True,
                 cache='mongodb://127.0.0.1:5000/persist')
        def square(x):
            return x*x
        square.clear()
        assert square(3) == 9
        with pytest.raises(HashCollisionError) as hce:
            square(4)
        assert hce.value.args[0] != hce.value.args[1]
    finally:
        mongo_process.kill()

def test_unhash():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])
    sleep(SLEEP_TIME)
    try:
        @persist(key=float,
                 hash=lambda k: 'e to the ' + str(k),
                 unhash=lambda s: float(s[9:]),
                 cache='mongodb://127.0.0.1:5000/persist')
        def exp(x):
            return 2.71828 ** x
        exp.clear()

        exp(2)
        exp(2.0)
        exp(-1)
        exp(3.14)
        assert len(exp.cache) == 3
        keys = [key for key in exp.cache]
        assert sorted(keys) == [-1, 2.0, 3.14]
        strings = ['e to the ' + str(item[0]) + ' equals ' + str(item[1])
                   for item in exp.cache.items()]
        assert [s[:30] for s in sorted(strings)] == [
            'e to the -1.0 equals 0.3678796',
            'e to the 2.0 equals 7.38904615',
            'e to the 3.14 equals 23.103818'
        ]
    finally:
        mongo_process.kill()

def test_unhash_collision():
    mongo_process = subprocess.Popen(['python', 'mongodb_server/run.py'])
    sleep(SLEEP_TIME)
    try:
        @persist(key=lambda x: x,
                 hash=lambda k: '16',  # same as hash=str for x==16 only
                 unhash=int,
                 cache='mongodb://127.0.0.1:5000/persist')
        def square(x):
            return x*x
        square.clear()

        assert square(16) == 256
        assert square(16) == 256
        with pytest.raises(HashCollisionError) as hce:
            square(12)
        assert hce.value.args == (16, 12)
    finally:
        mongo_process.kill()

def test_noserver():
    @persist(cache='mongodb://127.0.0.1:5000/doesntexist')
    def deg_to_rad(deg):
        return deg / 360 * 2 * 3.141592653589793

    with pytest.raises(ConnectionError) as ce:
        deg_to_rad.clear()
    with pytest.raises(ConnectionError) as re:
        assert deg_to_rad(90) == 3.141592653589793 / 2
