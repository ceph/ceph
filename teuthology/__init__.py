from gevent import monkey
monkey.patch_all(dns=False)
from .orchestra import monkey
monkey.patch_all()

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s')
log = logging.getLogger(__name__)
