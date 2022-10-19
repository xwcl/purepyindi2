import re
def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True

WHITESPACE_RE = re.compile(r"\s+")

def unwrap(message):
    return WHITESPACE_RE.sub(" ", message).strip()