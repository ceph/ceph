"""Utility to probe protobuf version and control behavior."""

from typing import Tuple


def protobuf_ver_tuple() -> Tuple[int, ...]:
    import google.protobuf

    ver = tuple(int(p) for p in google.protobuf.__version__.split('.'))
    return ver


def protobuf_choose_impl() -> None:
    """Work around the crummy old version of protobuf that is available on
    rhel10. Force the python implementation unless 3.20 or newer because
    we can monkeypatch the python version but not the c ext version.
    """
    import os

    key = 'PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'
    if key in os.environ:
        return
    ver = protobuf_ver_tuple()
    if ver[:2] >= (3, 20):
        return
    os.environ[key] = 'python'
