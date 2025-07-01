import gzip
import json
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from typing import Optional, Any, Tuple
import logging

logger = logging.getLogger()


def make_json_request(url: str, json_bytes: bytes, compress: bool) -> Optional[Request]:
    """
    Create a JSON POST request, optionally gzip-compressed. `json_bytes` must be JSON-encoded bytes.
    """
    if not json_bytes:
        logger.error("Cannot send empty JSON payload — returning empty Request.")
        return None

    if compress:
        original_size = len(json_bytes)
        json_bytes = gzip.compress(json_bytes)
        compressed_size = len(json_bytes)
        compression_ratio = 100 * (1 - compressed_size / original_size)
        logger.debug(f"Compression reduced payload size by {compression_ratio:.1f}% ({original_size} → {compressed_size} bytes)")
        headers = {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip',
            'Accept-Encoding': 'gzip',
        }
    else:
        headers = {
            'Content-Type': 'application/json',
        }
    return Request(url, data=json_bytes, headers=headers)


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
    logger.debug(f'Sending query to {url} (compression={compress})')
    try:
        req = make_json_request(url, data, compress) if data else Request(url)
        if req is not None:
            with urlopen(req, context=ssl_ctx, timeout=timeout) as response:
                response_str = response.read()
                response_status = response.status
        else:
            return (-1, "Empty payload — request not sent")

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
