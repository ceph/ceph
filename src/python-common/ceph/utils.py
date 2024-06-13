import datetime
import re
import string
import ssl

from typing import Optional, MutableMapping, Tuple, Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request

import logging

log = logging.getLogger(__name__)


def datetime_now() -> datetime.datetime:
    """
    Return the current local date and time.
    :return: Returns an aware datetime object of the current date
        and time.
    """
    return datetime.datetime.now(tz=datetime.timezone.utc)


def datetime_to_str(dt: datetime.datetime) -> str:
    """
    Convert a datetime object into a ISO 8601 string, e.g.
    '2019-04-24T17:06:53.039991Z'.
    :param dt: The datetime object to process.
    :return: Return a string representing the date in
        ISO 8601 (timezone=UTC).
    """
    return dt.astimezone(tz=datetime.timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')


def str_to_datetime(string: str) -> datetime.datetime:
    """
    Convert an ISO 8601 string into a datetime object.
    The following formats are supported:

    - 2020-03-03T09:21:43.636153304Z
    - 2020-03-03T15:52:30.136257504-0600
    - 2020-03-03T15:52:30.136257504

    :param string: The string to parse.
    :return: Returns an aware datetime object of the given date
        and time string.
    :raises: :exc:`~exceptions.ValueError` for an unknown
        datetime string.
    """
    fmts = [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S.%f%z'
    ]

    # In *all* cases, the 9 digit second precision is too much for
    # Python's strptime. Shorten it to 6 digits.
    p = re.compile(r'(\.[\d]{6})[\d]*')
    string = p.sub(r'\1', string)

    # Replace trailing Z with -0000, since (on Python 3.6.8) it
    # won't parse.
    if string and string[-1] == 'Z':
        string = string[:-1] + '-0000'

    for fmt in fmts:
        try:
            dt = datetime.datetime.strptime(string, fmt)
            # Make sure the datetime object is aware (timezone is set).
            # If not, then assume the time is in UTC.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt
        except ValueError:
            pass

    raise ValueError("Time data {} does not match one of the formats {}".format(
        string, str(fmts)))


def parse_timedelta(delta: str) -> Optional[datetime.timedelta]:
    """
    Returns a timedelta object represents a duration, the difference
    between two dates or times.

    >>> parse_timedelta('foo')

    >>> parse_timedelta('2d') == datetime.timedelta(days=2)
    True

    >>> parse_timedelta("4w") == datetime.timedelta(days=28)
    True

    >>> parse_timedelta("5s") == datetime.timedelta(seconds=5)
    True

    >>> parse_timedelta("-5s") == datetime.timedelta(days=-1, seconds=86395)
    True

    :param delta: The string to process, e.g. '2h', '10d', '30s'.
    :return: The `datetime.timedelta` object or `None` in case of
        a parsing error.
    """
    parts = re.match(r'(?P<seconds>-?\d+)s|'
                     r'(?P<minutes>-?\d+)m|'
                     r'(?P<hours>-?\d+)h|'
                     r'(?P<days>-?\d+)d|'
                     r'(?P<weeks>-?\d+)w$',
                     delta,
                     re.IGNORECASE)
    if not parts:
        return None
    parts = parts.groupdict()
    args = {name: int(param) for name, param in parts.items() if param}
    return datetime.timedelta(**args)


def is_hex(s: str, strict: bool = True) -> bool:
    """Simple check that a string contains only hex chars"""
    try:
        int(s, 16)
    except ValueError:
        return False

    # s is multiple chars, but we should catch a '+/-' prefix too.
    if strict:
        if s[0] not in string.hexdigits:
            return False

    return True


def http_req(hostname: str = '',
             port: str = '443',
             method: Optional[str] = None,
             headers: MutableMapping[str, str] = {},
             data: Optional[str] = None,
             endpoint: str = '/',
             scheme: str = 'https',
             ssl_verify: bool = False,
             timeout: Optional[int] = None,
             ssl_ctx: Optional[Any] = None) -> Tuple[Any, Any, Any]:

    if not ssl_ctx:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if not ssl_verify:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        else:
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED

    url: str = f'{scheme}://{hostname}:{port}{endpoint}'
    _data = bytes(data, 'ascii') if data else None
    _headers = headers
    if data and not method:
        method = 'POST'
    if not _headers.get('Content-Type') and method in ['POST', 'PATCH']:
        _headers['Content-Type'] = 'application/json'
    try:
        req = Request(url, _data, _headers, method=method)
        with urlopen(req, context=ssl_ctx, timeout=timeout) as response:
            response_str = response.read()
            response_headers = response.headers
            response_code = response.code
        return response_headers, response_str.decode(), response_code
    except (HTTPError, URLError) as e:
        log.error(e)
        # handle error here if needed
        raise
