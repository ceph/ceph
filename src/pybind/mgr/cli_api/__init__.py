from .module import CLI

__all__ = [
    "CLI",
]

import os
if 'UNITTEST' in os.environ:
    import tests  # noqa # pylint: disable=unused-import
    __all__.append(tests.__name__)
