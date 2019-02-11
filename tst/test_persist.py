import pytest

from pypersist import persist

from time import sleep
from os import listdir

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
