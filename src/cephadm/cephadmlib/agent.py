import gzip
import json
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from typing import Optional, Any, Tuple
import logging

logger = logging.getLogger()


def make_json_request(url: str, payload: dict, compress: bool) -> Request:
    """
    Create a JSON POST request, optionally gzip-compressed.
    """
    body = json.dumps(payload).encode('utf-8')
    original_size = len(body)
    if compress:
        body = gzip.compress(body)
        compressed_size = len(body)
        compression_ratio = 100 * (1 - compressed_size / original_size)
        logger.info(f"Compression reduced payload size by {compression_ratio:.1f}% ({original_size} → {compressed_size} bytes)")
        headers = {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip',
            'Accept-Encoding': 'gzip',
        }
    else:
        headers = {
            'Content-Type': 'application/json',
        }
    return Request(url, data=body, headers=headers)


def http_query(
    addr: str = '',
    port: str = '',
    data: Optional[bytes] = None,
    endpoint: str = '',
    ssl_ctx: Optional[Any] = None,
    timeout: Optional[int] = 10,
    compress: bool = False,
) -> Tuple[int, str]:
    """
    Perform an HTTPS POST request with optional gzip-compressed JSON payload.

    `data` should be a JSON-encoded bytes object (not already compressed).
    """
    url = f'https://{addr}:{port}{endpoint}'
    logger.debug(f'sending query to {url} (compression={compress})')
    try:
        if data:
            payload = json.loads(data.decode('utf-8'))
            req = make_json_request(url, payload, compress)
        else:
            req = Request(url)

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
        response_str = str(e.reason)
    except Exception as e:
        logger.error(f'Unexpected error: {e}')
        raise

    return (response_status, response_str.decode('utf-8') if isinstance(response_str, bytes) else response_str)
