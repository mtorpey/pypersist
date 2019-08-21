import pytest

from pypersist import persist
from threading import Thread
from time import sleep
from os.path import join, exists
from os import listdir


def test_threadsafe():
    def slow_pickle(val):
        sleep(1)
        return str(val)

    @persist(pickle=slow_pickle, unpickle=int, key=lambda x: x, hash=str)
    def double(x):
        return x * 2

    double.clear()

    threads = [Thread(target=double, args=(3,)) for i in range(5)]
    for t in threads:
        t.start()
    sleep(0.1)
    assert exists(join("persist", "double", "3.lock"))
    for t in threads:
        t.join()
    assert not exists(join("persist", "double", "3.lock"))

    assert exists(join("persist", "double", "3.out"))
    assert len(double.cache) == 1
    assert double(3) == 6
    assert len(double.cache) == 1
