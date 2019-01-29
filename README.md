# pypersist

[![PyPI version](https://badge.fury.io/py/pypersist.svg)](https://badge.fury.io/py/pypersist)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/mtorpey/pypersist/master?filepath=demo.ipynb)

Persistent memoisation framework for Python

Allows functions to store their outputs permanently to disk or to an online
database, in a configurable way.

## Examples
To use, import the `persist` class from the `pypersist` package:

```python
from pypersist import persist
```

and use it as a decorator when writing a function:

```python
@persist
def double(x):
    return x * 2

print(double(3))
print(double(6.5))
```

This will store the outputs of the `double` function in a directory called
`persist/double/`, in a machine-readable format.

One can specify various arguments to `persist`.  For example:

```python
@persist(key=lambda x,y: (x,y),
         hash=lambda k: '%s_to_the_power_of_%s' % k,
         pickle=str,
         unpickle=int)
def power(x, y):
    return x ** y

print(power(2,4))
print(power(10,5))
```

will store the outputs of `power` in human-readable files with descriptive
filenames.

Many more options are available.  See the documentation in `pypersist.py` for a
full description, or
[launch the included notebook on Binder](https://mybinder.org/v2/gh/mtorpey/pypersist/master?filepath=demo.ipynb)
for more examples.

See [this HackMD](https://hackmd.io/KSBMXDpZRbCKCKVZH7CJpg) and the Issue
tracker for current plans.
