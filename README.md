pypersist
=========

[![PyPI version](https://badge.fury.io/py/pypersist.svg)](https://badge.fury.io/py/pypersist)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/mtorpey/pypersist/master?filepath=demo.ipynb)

pypersist is a persistent memoisation framework for Python 3.  Persistent
memoisation is the practice of storing the output of a function permanently to a
disk or a server so that the result can be looked up automatically in the
future, avoiding any known results being recomputed unnecessarily.

Installation
------------
pypersist is available from PyPI, and the latest release can be installed using,
for example:

    pip3 install --user pypersist

Alternatively, the latest development version can be installed using Github:

    git clone https://github.com/mtorpey/pypersist.git
    pip3 install --user ./pypersist

Examples
--------
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

Many more options are available.  See the `persist` class documentation for a
full description, or [launch the included notebook on
Binder](https://mybinder.org/v2/gh/mtorpey/pypersist/master?filepath=demo.ipynb)
for more examples.

See [this HackMD](https://hackmd.io/1M5clex-TYWCuxxgi05k5A) and the Issue
tracker for current plans.

pypersist is a part of the OpenDreamKit project: https://opendreamkit.org/
