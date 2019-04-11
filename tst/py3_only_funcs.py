from pypersist import persist


@persist(cache='file://results_for_alice/', funcname='foofighters')
def foo(x, y, z=1, *, a=3):
    return x + y + z + a


def baz(a, b, c=3, d=4, *, e, f=6, g=7):
    return a+b+c+d+e+f+g
