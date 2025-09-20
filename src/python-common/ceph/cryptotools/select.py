from typing import Dict

import os

from .caller import CryptoCaller


_CC_ENV = 'CEPH_CRYPTOCALLER'
_CC_KEY = 'crypto_caller'
_CC_REMOTE = 'remote'
_CC_INTERNAL = 'internal'

_CACHE: Dict[str, CryptoCaller] = {}


def _check_name(name: str) -> None:
    if name and name not in (_CC_REMOTE, _CC_INTERNAL):
        raise ValueError(f'unexpected crypto caller name: {name}')


def choose_crypto_caller(name: str = '') -> None:
    _check_name(name)
    if not name:
        name = os.environ.get(_CC_ENV, '')
        _check_name(name)
    if not name:
        name = _CC_REMOTE

    if name == _CC_REMOTE:
        import ceph.cryptotools.remote

        _CACHE[_CC_KEY] = ceph.cryptotools.remote.ProcessCryptoCaller()
        return
    if name == _CC_INTERNAL:
        import ceph.cryptotools.internal

        _CACHE[_CC_KEY] = ceph.cryptotools.internal.InternalCryptoCaller()
        return
    # should be unreachable
    raise RuntimeError('failed to setup a valid crypto caller')


def get_crypto_caller() -> CryptoCaller:
    """Return the currently selected crypto caller object."""
    caller = _CACHE.get(_CC_KEY)
    if not caller:
        choose_crypto_caller()
        caller = _CACHE.get(_CC_KEY)
        if caller is None:
            raise RuntimeError('failed to select crypto caller')
    return caller
