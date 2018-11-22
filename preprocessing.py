from inspect import getfullargspec


def arg_tuple(func, *args, **kwargs):
    kwargs = kwargs.copy()

    spec = getfullargspec(func)

    # Check for too many arguments
    if len(args) > len(spec.args):
        func(*args, **kwargs)  # throws TypeError with useful message

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
