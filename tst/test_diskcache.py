import pytest

from pypersist import persist
from threading import Thread
from time import sleep
from os.path import join, exists
from os import remove


def test_threadsafe():
    @persist(pickle=str, unpickle=int, key=lambda x: x, hash=str)
    def double(x):
        return x * 2

    double.clear()
    assert len(double.cache) == 0

    threads = [Thread(target=double, args=(3,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(double.cache) == 1

    threads = [Thread(target=double, args=(3,)) for i in range(5)]
    lockfname = join("persist", "double", "3.lock")
    assert not exists(lockfname)
    open(lockfname, "w").close()  # lock the result
    for t in threads:
        t.start()
    for t in threads:
        assert t.is_alive()
    assert exists(lockfname)
    remove(lockfname)  # unlock the result
    for t in threads:
        t.join()
    for t in threads:
        assert not t.is_alive()
    assert not exists(lockfname)

    assert exists(join("persist", "double", "3.out"))
    assert len(double.cache) == 1
    assert double(3) == 6
    assert len(double.cache) == 1
