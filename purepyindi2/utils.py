def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True