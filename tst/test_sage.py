import pytest

from pypersist import persist
from pypersist.commoncache import HashCollisionError

from os import listdir
from os.path import join, exists
from sys import version_info
from datetime import datetime

from sys import modules

try:
    from sage.all import *
    SAGE=True
except ImportError:
    SAGE=False

def test_triple():
    if not SAGE:
        return

    assert dumps

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
    del triple.cache[(('x', 3),)]
    assert len(triple.cache) == 1
    with pytest.raises(KeyError) as ke:
        del triple.cache[(('x', 5),)]
    assert ke.value.args[0] == (('x', 5),)


def test_identity():
    if not SAGE:
        return

    @persist
    def identity(x):
        return x

    assert identity('string') == 'string'
    assert identity(int(1)) == int(1)
    assert identity(1) == 1
    assert identity(1/2) == 1/2

    assert identity('string') == 'string'
    assert identity(int(1)) == int(1)
    assert identity(1) == 1
    assert identity(1/2) == 1/2

    M = matrix(2)
    assert identity(M) == M

    R = PolynomialRing(QQ, 't')
    t = R.gen()
    assert identity(R) == R
    assert identity(R) == R
    assert identity(t) == t
    assert identity(t) == t

    var('a')
    assert identity(a) == a


def test_fact():
    if not SAGE:
        return

    @persist
    def fact(x):
        return factor(x)

    var('a')
    p = (a**2-1)
    assert fact(p) == p.factor()
    assert fact(p) == p.factor()
    assert fact(8) == factor(8)
    assert fact(8) == factor(8)

    R = PolynomialRing(QQ, 't')
    t = R.gen()
    q = t**2-1
    assert fact(q) == q.factor()
    assert fact(q) == q.factor()
