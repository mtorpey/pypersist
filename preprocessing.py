from inspect import getfullargspec


def arg_tuple(func, *args, **kwargs):
    kwargs.update(dict(zip(getfullargspec(func).args, args)))
    out = tuple(sorted(kwargs.items()))
    return out
