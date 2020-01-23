pypersist
=========

[![Build Status](https://travis-ci.org/mtorpey/pypersist.svg?branch=master)](https://travis-ci.org/mtorpey/pypersist)
[![codecov](https://codecov.io/gh/mtorpey/pypersist/branch/master/graph/badge.svg)](https://codecov.io/gh/mtorpey/pypersist)
[![Documentation Status](https://readthedocs.org/projects/pypersist/badge/?version=latest)](https://pypersist.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/pypersist.svg)](https://badge.fury.io/py/pypersist)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/mtorpey/pypersist/master?filepath=binder/demo.ipynb)

pypersist is a persistent memoisation framework for Python 2 and 3.  Persistent
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
Binder](https://mybinder.org/v2/gh/mtorpey/pypersist/master?filepath=binder/demo.ipynb)
for more examples.

See [this HackMD](https://hackmd.io/1M5clex-TYWCuxxgi05k5A) and the Issue
tracker for current plans.

Citing
------
Please cite this package as:

[Tor20] 
M. Torpey, 
pypersist, 
Python memoisation framework,
Version X.Y (20XX),
https://github.com/mtorpey/pypersist.

Acknowledgements
----------------
pypersist was created as part of the OpenDreamKit project: 
https://opendreamkit.org/

This part of the project is summarised in [this report](https://github.com/OpenDreamKit/OpenDreamKit/blob/master/WP6/D6.9/report-final.pdf).

<table class="none">
<tr>
<td>
  <img src="http://opendreamkit.org/public/logos/Flag_of_Europe.svg" width="128">
</td>
<td>
  This infrastructure is part of a project that has received funding from the
  European Union's Horizon 2020 research and innovation programme under grant
  agreement No 676541.
</td>
</tr>
</table>
