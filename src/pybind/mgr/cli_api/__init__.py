import os

if 'UNITTEST' in os.environ:
    import tests  # noqa # pylint: disable=unused-import
else:
    from .module import CLI  # noqa # pylint: disable=unused-import