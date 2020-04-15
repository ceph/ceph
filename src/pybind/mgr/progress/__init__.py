import os
if 'UNITTEST' not in os.environ:
    from .module import *
else:
    import tests


