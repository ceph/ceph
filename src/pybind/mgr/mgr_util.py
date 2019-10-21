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


def format_dimless(n, width, colored=True):
    return format_units(n, width, colored, decimal=True)


def format_bytes(n, width, colored=True):
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
