from inspect import getfullargspec


def arg_tuple(func, *args, **kwargs):
    spec = getfullargspec(func)

    # Convert args to kwargs
    kwargs.update(dict(zip(spec.args, args)))

    # Remove any default arguments
    if not spec.defaults is None:
        for (arg, val) in zip(spec.args[-len(spec.defaults):], spec.defaults):
            if kwargs.get(arg) == val:
                kwargs.pop(arg)
    if not spec.kwonlydefaults is None:
        for arg in spec.kwonlydefaults:
            if kwargs.get(arg) == spec.kwonlydefaults[arg]:
                kwargs.pop(arg)

    # Return as a tuple
    out = tuple(sorted(kwargs.items()))
    return out
