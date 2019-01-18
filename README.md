# pymemo

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/mtorpey/pymemo/master?filepath=Persistent%20memoisation.ipynb)

Persistent memoisation framework for Python

Allows functions to store their outputs permanently to disk (and in the future,
to a database), in a configurable way.

## Examples
To use, import the `persist` class from `pymemo.py`:

```python
from pymemo import persist
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

Many more options are available.  See the documentation in `pymemo.py` for a
full description, or
[launch the included notebook on Binder](https://mybinder.org/v2/gh/mtorpey/pymemo/master?filepath=Persistent%20memoisation.ipynb)
for more examples.

