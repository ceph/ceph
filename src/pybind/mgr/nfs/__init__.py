# flake8: noqa

import typing
import os
if 'UNITTEST' in os.environ:
    import tests


def _get_ceph_mgr_module_class(name: str) -> typing.Any:
    "Return a MgrModule subclass for the ceph mgr."
    from .module import Module

    return Module
