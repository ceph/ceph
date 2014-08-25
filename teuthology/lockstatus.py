import requests
import os
from .config import config
from .misc import canonicalize_hostname


def get_status(name):
    name = canonicalize_hostname(name, user=None)
    uri = os.path.join(config.lock_server, 'nodes', name, '')
    response = requests.get(uri)
    success = response.ok
    if success:
        return response.json()
    return None
