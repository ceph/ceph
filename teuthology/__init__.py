from gevent import monkey
monkey.patch_all(dns=False)
from .orchestra import monkey
monkey.patch_all()
