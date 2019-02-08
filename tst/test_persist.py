from pypersist import persist

import pypersist.preprocessing

@persist
def double(x):
    return 2 * x

double.clear()

assert len(double.cache) == 0
assert double(3) == 6
assert len(double.cache) == 1
assert double.cache
assert double.cache[(('x', 3),)] == 6
