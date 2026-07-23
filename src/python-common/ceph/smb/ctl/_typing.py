"""Typing helpers"""

import typing

import sys

if sys.version_info >= (3, 11):
    from typing import Self
elif typing.TYPE_CHECKING:
    from typing_extensions import Self
else:
    Self = typing.Any
