import pytest

from pypersist import persist

from os import listdir
from os.path import join

def test_triple():
    @persist
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

def test_pickle():
    @persist(pickle=repr, unpickle=eval)
    def double(x):
        return 2 * x
    double.clear()

    assert double(2) == 4
    assert double(2) == 4
    assert len(double.cache) == 1
    assert double(x=2) == 4
    assert len(double.cache) == 1
    assert double(0) == 0
    assert double('hello!') == 'hello!hello!'
    assert len(double.cache) == 3

    files = listdir('persist/double')
    results = [open('persist/double/' + fname).read() for fname in files]
    assert '4' in results
    assert '0' in results
    assert "'hello!hello!'" in results
    assert 'some random string' not in results

def test_locations():
    @persist(cache='file://results_for_alice/', funcname='foofighters')
    def foo(x, y, z=1, *, a=3):
        return x + y + z + a
    foo.clear()

    assert foo(1,4,z=3) == 11
    assert foo(1,y=4,z=3) == 11
    assert foo(1,z=3,y=4) == 11
    assert foo(1,4,3,a=3) == 11  # Last arg has the default value, so it is ignored
    assert foo(1,4,3,a=7) == 15  # Last arg is kw-only, and used
    assert foo(1,4,a=3,z=3) == 11  # Default arg in non-canonical order
    assert len(foo.cache) == 2

    assert len(listdir(join('results_for_alice', 'foofighters'))) >= 2

def test_key():
    @persist(key=lambda *args: sorted(args))
    def sum(*args):
        acc = 0
        for x in args:
            acc += x
        return acc
    sum.clear()

    assert sum(1,4,3,7,3,12) == 30
    assert len(sum.cache) == 1
    assert sum(4,12,7,3,3,1) == 30
    assert len(sum.cache) == 1
    assert sum.cache[[1,3,3,4,7,12]] == 30
    with pytest.raises(KeyError) as ke:
        sum.cache[[1,4,3,7,3,12]]
    assert ke.value.args[0] == [1,4,3,7,3,12]

def test_hash():
    @persist(hash=lambda k: '%s to the %s' % (k[0][1], k[1][1]),
             pickle=str,
             unpickle=int)
    def pow(x,y):
        return x**y
    pow.clear()

    assert pow(2,3) == 8
    assert pow(7,4) == 2401
    assert pow(1,3) == 1
    assert pow(10,5) == 100000
    assert pow(0,0) == 1
    assert pow(2,16) == 65536

    fnames = listdir('persist/pow')
    assert sorted(fnames) == ['0 to the 0.out',
                              '1 to the 3.out',
                              '10 to the 5.out',
                              '2 to the 16.out',
                              '2 to the 3.out',
                              '7 to the 4.out']
    assert open('persist/pow/7 to the 4.out', 'r').read() == '2401'
    assert open('persist/pow/0 to the 0.out', 'r').read() == '1'
