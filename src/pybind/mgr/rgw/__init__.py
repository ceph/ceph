try:
    from .module import Module
except ImportError:
    pass

import logging

log = logging.getLogger(__name__)
