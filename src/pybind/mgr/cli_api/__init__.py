import os
from .module import CLI  # noqa # pylint: disable=unused-import


if 'UNITTEST' in os.environ:
    import tests  # noqa # pylint: disable=unused-import
