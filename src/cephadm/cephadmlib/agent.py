from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from typing import Optional, Any, Tuple
import importlib
import logging

logger = logging.getLogger()


def http_query(
    addr: str = '',
    port: str = '',
    data: Optional[bytes] = None,
    endpoint: str = '',
    ssl_ctx: Optional[Any] = None,
    timeout: Optional[int] = 10,
) -> Tuple[int, str]:
    url = f'https://{addr}:{port}{endpoint}'
    logger.debug(f'sending query to {url}')
    try:
        req = Request(url, data, {'Content-Type': 'application/json'})
        with urlopen(req, context=ssl_ctx, timeout=timeout) as response:
            response_str = response.read()
            response_status = response.status
    except HTTPError as e:
        logger.debug(f'{e.code} {e.reason}')
        response_status = e.code
        response_str = e.reason
    except URLError as e:
        logger.debug(f'{e.reason}')
        response_status = -1
        response_str = e.reason
    except Exception:
        raise
    return (response_status, response_str)


def _load_version_module() -> Optional[object]:
    """Import and return the cephadm version module.

    Tries ``_cephadmmeta.version`` first (the current bundled location),
    then falls back to the legacy ``_version`` module.  Returns ``None``
    when neither is importable.

    Note: the ``zmod``/zipimport path used by ``command_version`` for
    verbose output is intentionally omitted here; we only need the version
    string.
    """
    try:
        return importlib.import_module('_cephadmmeta.version')
    except ImportError:
        pass
    try:
        return importlib.import_module('_version')
    except ImportError:
        return None


def get_agent_version() -> Optional[str]:
    """Return the ``CEPH_GIT_NICE_VER`` string for the running cephadm.

    Uses :func:`_load_version_module` to locate the version module.
    Returns ``None`` when the version cannot be determined.
    """
    vmod = _load_version_module()
    if vmod is not None:
        return getattr(vmod, 'CEPH_GIT_NICE_VER', None)
    return None
