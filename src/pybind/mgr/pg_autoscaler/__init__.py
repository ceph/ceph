import os

if 'UNITTEST' in os.environ:
    import tests

from .module import PgAutoscaler, effective_target_ratio
