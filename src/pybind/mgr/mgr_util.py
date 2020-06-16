import contextlib
import os
import socket
import logging

(
    BLACK,
    RED,
    GREEN,
    YELLOW,
    BLUE,
    MAGENTA,
    CYAN,
    GRAY
) = range(8)

RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
COLOR_DARK_SEQ = "\033[0;%dm"
BOLD_SEQ = "\033[1m"
UNDERLINE_SEQ = "\033[4m"

logger = logging.getLogger(__name__)


def colorize(msg, color, dark=False):
    """
    Decorate `msg` with escape sequences to give the requested color
    """
    return (COLOR_DARK_SEQ if dark else COLOR_SEQ) % (30 + color) \
        + msg + RESET_SEQ


def bold(msg):
    """
    Decorate `msg` with escape sequences to make it appear bold
    """
    return BOLD_SEQ + msg + RESET_SEQ


def format_units(n, width, colored, decimal):
    """
    Format a number without units, so as to fit into `width` characters, substituting
    an appropriate unit suffix.

    Use decimal for dimensionless things, use base 2 (decimal=False) for byte sizes/rates.
    """

    factor = 1000 if decimal else 1024
    units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
    unit = 0
    while len("%s" % (int(n) // (factor**unit))) > width - 1:
        unit += 1

    if unit > 0:
        truncated_float = ("%f" % (n / (float(factor) ** unit)))[0:width - 1]
        if truncated_float[-1] == '.':
            truncated_float = " " + truncated_float[0:-1]
    else:
        truncated_float = "%{wid}d".format(wid=width - 1) % n
    formatted = "%s%s" % (truncated_float, units[unit])

    if colored:
        if n == 0:
            color = BLACK, False
        else:
            color = YELLOW, False
        return bold(colorize(formatted[0:-1], color[0], color[1])) \
            + bold(colorize(formatted[-1], BLACK, False))
    else:
        return formatted


def format_dimless(n, width, colored=False):
    return format_units(n, width, colored, decimal=True)


def format_bytes(n, width, colored=False):
    return format_units(n, width, colored, decimal=False)


def merge_dicts(*args):
    # type: (dict) -> dict
    """
    >>> assert merge_dicts({1:2}, {3:4}) == {1:2, 3:4}
        You can also overwrite keys:
    >>> assert merge_dicts({1:2}, {1:4}) == {1:4}
    :rtype: dict[str, Any]
    """
    ret = {}
    for arg in args:
        ret.update(arg)
    return ret


def get_default_addr():
    def is_ipv6_enabled():
        try:
            sock = socket.socket(socket.AF_INET6)
            with contextlib.closing(sock):
                sock.bind(("::1", 0))
                return True
        except (AttributeError, socket.error) as e:
           return False

    try:
        return get_default_addr.result
    except AttributeError:
        result = '::' if is_ipv6_enabled() else '0.0.0.0'
        get_default_addr.result = result
        return result


class ServerConfigException(Exception):
    pass

def verify_cacrt(cert_fname):
    """Basic validation of a ca cert"""

    if not cert_fname:
        raise ServerConfigException("CA cert not configured")
    if not os.path.isfile(cert_fname):
        raise ServerConfigException("Certificate {} does not exist".format(cert_fname))

    from OpenSSL import crypto
    try:
        with open(cert_fname) as f:
            x509 = crypto.load_certificate(crypto.FILETYPE_PEM, f.read())
            if x509.has_expired():
                logger.warning(
                    'Certificate {} has expired'.format(cert_fname))
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(
            'Invalid certificate {}: {}'.format(cert_fname, str(e)))


def verify_tls_files(cert_fname, pkey_fname):
    """Basic checks for TLS certificate and key files

    Do some validations to the private key and certificate:
    - Check the type and format
    - Check the certificate expiration date
    - Check the consistency of the private key
    - Check that the private key and certificate match up

    :param cert_fname: Name of the certificate file
    :param pkey_fname: name of the certificate public key file

    :raises ServerConfigException: An error with a message

    """

    if not cert_fname or not pkey_fname:
        raise ServerConfigException('no certificate configured')

    verify_cacrt(cert_fname)

    if not os.path.isfile(pkey_fname):
        raise ServerConfigException('private key %s does not exist' % pkey_fname)

    from OpenSSL import crypto, SSL

    try:
        with open(pkey_fname) as f:
            pkey = crypto.load_privatekey(crypto.FILETYPE_PEM, f.read())
            pkey.check()
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(
            'Invalid private key {}: {}'.format(pkey_fname, str(e)))
    try:
        context = SSL.Context(SSL.TLSv1_METHOD)
        context.use_certificate_file(cert_fname, crypto.FILETYPE_PEM)
        context.use_privatekey_file(pkey_fname, crypto.FILETYPE_PEM)
        context.check_privatekey()
    except crypto.Error as e:
        logger.warning(
            'Private key {} and certificate {} do not match up: {}'.format(
                pkey_fname, cert_fname, str(e)))

def get_most_recent_rate(rates):
    """ Get most recent rate from rates

    :param rates: The derivative between all time series data points [time in seconds, value]
    :type rates: list[tuple[int, float]]

    :return: The last derivative or 0.0 if none exists
    :rtype: float

    >>> get_most_recent_rate(None)
    0.0
    >>> get_most_recent_rate([])
    0.0
    >>> get_most_recent_rate([(1, -2.0)])
    -2.0
    >>> get_most_recent_rate([(1, 2.0), (2, 1.5), (3, 5.0)])
    5.0
    """
    if not rates:
        return 0.0
    return rates[-1][1]

def get_time_series_rates(data):
    """ Rates from time series data

    :param data: Time series data [time in seconds, value]
    :type data: list[tuple[int, float]]

    :return: The derivative between all time series data points [time in seconds, value]
    :rtype: list[tuple[int, float]]

    >>> logger.debug = lambda s,x,y: print(s % (x,y))
    >>> get_time_series_rates([])
    []
    >>> get_time_series_rates([[0, 1], [1, 3]])
    [(1, 2.0)]
    >>> get_time_series_rates([[0, 2], [0, 3], [0, 1], [1, 2], [1, 3]])
    Duplicate timestamp in time series data: [0, 2], [0, 3]
    Duplicate timestamp in time series data: [0, 3], [0, 1]
    Duplicate timestamp in time series data: [1, 2], [1, 3]
    [(1, 2.0)]
    >>> get_time_series_rates([[1, 1], [2, 3], [4, 11], [5, 16], [6, 22]])
    [(2, 2.0), (4, 4.0), (5, 5.0), (6, 6.0)]
    """
    data = _filter_time_series(data)
    if not data:
        return []
    return [(data2[0], _derivative(data1, data2)) for data1, data2 in
            _pairwise(data)]

def _filter_time_series(data):
    """ Filters time series data

    Filters out samples with the same timestamp in given time series data.
    It also enforces the list to contain at least two samples.

    All filtered values will be shown in the debug log. If values were filtered it's a bug in the
    time series data collector, please report it.

    :param data: Time series data [time in seconds, value]
    :type data: list[tuple[int, float]]

    :return: Filtered time series data [time in seconds, value]
    :rtype: list[tuple[int, float]]

    >>> logger.debug = lambda s,x,y: print(s % (x,y))
    >>> _filter_time_series([])
    []
    >>> _filter_time_series([[1, 42]])
    []
    >>> _filter_time_series([[10, 2], [10, 3]])
    Duplicate timestamp in time series data: [10, 2], [10, 3]
    []
    >>> _filter_time_series([[0, 1], [1, 2]])
    [[0, 1], [1, 2]]
    >>> _filter_time_series([[0, 2], [0, 3], [0, 1], [1, 2], [1, 3]])
    Duplicate timestamp in time series data: [0, 2], [0, 3]
    Duplicate timestamp in time series data: [0, 3], [0, 1]
    Duplicate timestamp in time series data: [1, 2], [1, 3]
    [[0, 1], [1, 3]]
    >>> _filter_time_series([[1, 1], [2, 3], [4, 11], [5, 16], [6, 22]])
    [[1, 1], [2, 3], [4, 11], [5, 16], [6, 22]]
    """
    filtered = []
    for i in range(len(data) - 1):
        if data[i][0] == data[i + 1][0]:  # Same timestamp
            logger.debug("Duplicate timestamp in time series data: %s, %s", data[i], data[i + 1])
            continue
        filtered.append(data[i])
    if not filtered:
        return []
    filtered.append(data[-1])
    return filtered

def _derivative(p1, p2):
    """ Derivative between two time series data points

    :param p1: Time series data [time in seconds, value]
    :type p1: tuple[int, float]
    :param p2: Time series data [time in seconds, value]
    :type p2: tuple[int, float]

    :return: Derivative between both points
    :rtype: float

    >>> _derivative([0, 0], [2, 1])
    0.5
    >>> _derivative([0, 1], [2, 0])
    -0.5
    >>> _derivative([0, 0], [3, 1])
    0.3333333333333333
    """
    return (p2[1] - p1[1]) / float(p2[0] - p1[0])

def _pairwise(iterable):
    it = iter(iterable)
    a = next(it, None)

    for b in it:
        yield (a, b)
        a = b
