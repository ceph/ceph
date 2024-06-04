from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from typing import Optional, Any, Tuple
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
