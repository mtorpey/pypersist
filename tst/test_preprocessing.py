import pytest

from pypersist.preprocessing import arg_tuple

from sys import version_info

PYTHON_VERSION = version_info[0]  # major version number


def test_baz():
    if PYTHON_VERSION >= 3:
        from py3_only_funcs import baz
    else:
        def baz(a, b, c=3, d=4, e=0, f=6, g=7):
            return a+b+c+d+e+f+g

    assert(arg_tuple(baz, 10, 2, 3, e=50, g=3, f=6) ==
           (("a", 10), ("b", 2), ("e", 50), ("g", 3)))
    if PYTHON_VERSION >= 3:
        with pytest.raises(TypeError) as te:
            arg_tuple(baz, 1, 2, 3, 4, 5)
        assert(str(te.value) ==
               "baz() takes from 2 to 4 positional arguments but 5 were given")
    with pytest.raises(TypeError) as te:
        arg_tuple(baz, 1, 2, 3, 4, 5, 6, 7, 8)


def test_varargs():
    def foo(*someargs):
        return sum(someargs)
    assert arg_tuple(foo, 1, 2, 3) == (("*someargs", (1, 2, 3)),)

    def bar(x, y, *someargs):
        return 3*x + 2*y + sum(args)
    assert(arg_tuple(bar, 8, 7, 1, 2, 3) ==
           (("*someargs", (1, 2, 3)), ("x", 8), ("y", 7)))
