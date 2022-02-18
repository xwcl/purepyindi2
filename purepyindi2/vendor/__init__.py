import sys
if sys.version_info >= (3, 10, 0):
    import dataclasses
else:
    from . import py310_dataclasses as dataclasses

__all__ = ['dataclasses']