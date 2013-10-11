from gevent import monkey
monkey.patch_all(dns=False)
from .orchestra import monkey
monkey.patch_all()

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
