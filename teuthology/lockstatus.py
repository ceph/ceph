import requests
import os
from .config import config


def get_status(name):
    uri = os.path.join(config.lock_server, 'nodes', name, '')
    response = requests.get(uri)
    success = response.ok
    if success:
        return response.json()
    return None
