import cephfs
import contextlib
import datetime
import errno
import os
import socket
import time
import logging
import sys
from threading import Lock, Condition, Event
from typing import no_type_check
from functools import wraps
if sys.version_info >= (3, 3):
    from threading import Timer
else:
    from threading import _Timer as Timer

try:
    from typing import Tuple, Any, Callable
except ImportError:
    TYPE_CHECKING = False  # just for type checking

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


class CephfsConnectionException(Exception):
    def __init__(self, error_code, error_message):
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self):
        return self.errno, "", self.error_str

    def __str__(self):
        return "{0} ({1})".format(self.errno, self.error_str)


class CephfsConnectionPool(object):
    class Connection(object):
        def __init__(self, mgr, fs_name):
            self.fs = None
            self.mgr = mgr
            self.fs_name = fs_name
            self.ops_in_progress = 0
            self.last_used = time.time()
            self.fs_id = self.get_fs_id()

        def get_fs_id(self):
            fs_map = self.mgr.get('fs_map')
            for fs in fs_map['filesystems']:
                if fs['mdsmap']['fs_name'] == self.fs_name:
                    return fs['id']
            raise CephfsConnectionException(
                -errno.ENOENT, "FS '{0}' not found".format(self.fs_name))

        def get_fs_handle(self):
            self.last_used = time.time()
            self.ops_in_progress += 1
            return self.fs

        def put_fs_handle(self, notify):
            assert self.ops_in_progress > 0
            self.ops_in_progress -= 1
            if self.ops_in_progress == 0:
                notify()

        def del_fs_handle(self, waiter):
            if waiter:
                while self.ops_in_progress != 0:
                    waiter()
            if self.is_connection_valid():
                self.disconnect()
            else:
                self.abort()

        def is_connection_valid(self):
            fs_id = None
            try:
                fs_id = self.get_fs_id()
            except:
                # the filesystem does not exist now -- connection is not valid.
                pass
            logger.debug("self.fs_id={0}, fs_id={1}".format(self.fs_id, fs_id))
            return self.fs_id == fs_id

        def is_connection_idle(self, timeout):
            return (self.ops_in_progress == 0 and ((time.time() - self.last_used) >= timeout))

        def connect(self):
            assert self.ops_in_progress == 0
            logger.debug("Connecting to cephfs '{0}'".format(self.fs_name))
            self.fs = cephfs.LibCephFS(rados_inst=self.mgr.rados)
            logger.debug("Setting user ID and group ID of CephFS mount as root...")
            self.fs.conf_set("client_mount_uid", "0")
            self.fs.conf_set("client_mount_gid", "0")
            logger.debug("CephFS initializing...")
            self.fs.init()
            logger.debug("CephFS mounting...")
            self.fs.mount(filesystem_name=self.fs_name.encode('utf-8'))
            logger.debug("Connection to cephfs '{0}' complete".format(self.fs_name))
            self.mgr._ceph_register_client(self.fs.get_addrs())

        def disconnect(self):
            try:
                assert self.fs
                assert self.ops_in_progress == 0
                logger.info("disconnecting from cephfs '{0}'".format(self.fs_name))
                addrs = self.fs.get_addrs()
                self.fs.shutdown()
                self.mgr._ceph_unregister_client(addrs)
                self.fs = None
            except Exception as e:
                logger.debug("disconnect: ({0})".format(e))
                raise

        def abort(self):
            assert self.fs
            assert self.ops_in_progress == 0
            logger.info("aborting connection from cephfs '{0}'".format(self.fs_name))
            self.fs.abort_conn()
            logger.info("abort done from cephfs '{0}'".format(self.fs_name))
            self.fs = None

    class RTimer(Timer):
        """
        recurring timer variant of Timer
        """
        @no_type_check
        def run(self):
            try:
                while not self.finished.is_set():
                    self.finished.wait(self.interval)
                    self.function(*self.args, **self.kwargs)
                self.finished.set()
            except Exception as e:
                logger.error("CephfsConnectionPool.RTimer: %s", e)
                raise

    # TODO: make this configurable
    TIMER_TASK_RUN_INTERVAL = 30.0   # seconds
    CONNECTION_IDLE_INTERVAL = 60.0  # seconds

    def __init__(self, mgr):
        self.mgr = mgr
        self.connections = {}
        self.lock = Lock()
        self.cond = Condition(self.lock)
        self.timer_task = CephfsConnectionPool.RTimer(
            CephfsConnectionPool.TIMER_TASK_RUN_INTERVAL,
            self.cleanup_connections)
        self.timer_task.start()

    def cleanup_connections(self):
        with self.lock:
            logger.info("scanning for idle connections..")
            idle_fs = [fs_name for fs_name, conn in self.connections.items()
                       if conn.is_connection_idle(CephfsConnectionPool.CONNECTION_IDLE_INTERVAL)]
            for fs_name in idle_fs:
                logger.info("cleaning up connection for '{}'".format(fs_name))
                self._del_fs_handle(fs_name)

    def get_fs_handle(self, fs_name):
        with self.lock:
            conn = None
            try:
                conn = self.connections.get(fs_name, None)
                if conn:
                    if conn.is_connection_valid():
                        return conn.get_fs_handle()
                    else:
                        # filesystem id changed beneath us (or the filesystem does not exist).
                        # this is possible if the filesystem got removed (and recreated with
                        # same name) via "ceph fs rm/new" mon command.
                        logger.warning("filesystem id changed for volume '{0}', reconnecting...".format(fs_name))
                        self._del_fs_handle(fs_name)
                conn = CephfsConnectionPool.Connection(self.mgr, fs_name)
                conn.connect()
            except cephfs.Error as e:
                # try to provide a better error string if possible
                if e.args[0] == errno.ENOENT:
                    raise CephfsConnectionException(
                        -errno.ENOENT, "FS '{0}' not found".format(fs_name))
                raise CephfsConnectionException(-e.args[0], e.args[1])
            self.connections[fs_name] = conn
            return conn.get_fs_handle()

    def put_fs_handle(self, fs_name):
        with self.lock:
            conn = self.connections.get(fs_name, None)
            if conn:
                conn.put_fs_handle(notify=lambda: self.cond.notifyAll())

    def _del_fs_handle(self, fs_name, wait=False):
        conn = self.connections.pop(fs_name, None)
        if conn:
            conn.del_fs_handle(waiter=None if not wait else lambda: self.cond.wait())

    def del_fs_handle(self, fs_name, wait=False):
        with self.lock:
            self._del_fs_handle(fs_name, wait)

    def del_all_handles(self):
        with self.lock:
            for fs_name in list(self.connections.keys()):
                logger.info("waiting for pending ops for '{}'".format(fs_name))
                self._del_fs_handle(fs_name, wait=True)
                logger.info("pending ops completed for '{}'".format(fs_name))
            # no new connections should have been initialized since its
            # guarded on shutdown.
            assert len(self.connections) == 0


class CephfsClient(object):
    def __init__(self, mgr):
        self.mgr = mgr
        self.stopping = Event()
        self.connection_pool = CephfsConnectionPool(self.mgr)

    def is_stopping(self):
        return self.stopping.is_set()

    def shutdown(self):
        logger.info("shutting down")
        # first, note that we're shutting down
        self.stopping.set()
        # second, delete all libcephfs handles from connection pool
        self.connection_pool.del_all_handles()


@contextlib.contextmanager
def open_filesystem(fsc, fs_name):
    """
    Open a volume with shared access.
    This API is to be used as a context manager.

    :param fsc: cephfs client instance
    :param fs_name: fs name
    :return: yields a fs handle (ceph filesystem handle)
    """
    if fsc.is_stopping():
        raise CephfsConnectionException(-errno.ESHUTDOWN,
                                        "shutdown in progress")

    fs_handle = fsc.connection_pool.get_fs_handle(fs_name)
    try:
        yield fs_handle
    finally:
        fsc.connection_pool.put_fs_handle(fs_name)


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
    >>> merge_dicts({1:2}, {3:4})
    {1: 2, 3: 4}

    You can also overwrite keys:
    >>> merge_dicts({1:2}, {1:4})
    {1: 4}

    :rtype: dict[str, Any]
    """
    ret = {}
    for arg in args:
        ret.update(arg)
    return ret


def get_default_addr():
    # type: () -> str
    def is_ipv6_enabled():
        try:
            sock = socket.socket(socket.AF_INET6)
            with contextlib.closing(sock):
                sock.bind(("::1", 0))
                return True
        except (AttributeError, socket.error) as e:
           return False

    try:
        return get_default_addr.result  # type: ignore
    except AttributeError:
        result = '::' if is_ipv6_enabled() else '0.0.0.0'
        get_default_addr.result = result  # type: ignore
        return result


class ServerConfigException(Exception):
    pass


def create_self_signed_cert(organisation='Ceph', common_name='mgr') -> Tuple[str, str]:
    """Returns self-signed PEM certificates valid for 10 years.
    :return cert, pkey
    """

    from OpenSSL import crypto
    from uuid import uuid4

    # create a key pair
    pkey = crypto.PKey()
    pkey.generate_key(crypto.TYPE_RSA, 2048)

    # create a self-signed cert
    cert = crypto.X509()
    cert.get_subject().O = organisation
    cert.get_subject().CN = common_name
    cert.set_serial_number(int(uuid4()))
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)  # 10 years
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(pkey)
    cert.sign(pkey, 'sha512')

    cert = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
    pkey = crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey)

    return cert.decode('utf-8'), pkey.decode('utf-8')


def verify_cacrt_content(crt):
    # type: (str) -> None
    from OpenSSL import crypto
    try:
        x509 = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
        if x509.has_expired():
            logger.warning('Certificate has expired: {}'.format(crt))
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(
            'Invalid certificate: {}'.format(str(e)))


def verify_cacrt(cert_fname):
    # type: (str) -> None
    """Basic validation of a ca cert"""

    if not cert_fname:
        raise ServerConfigException("CA cert not configured")
    if not os.path.isfile(cert_fname):
        raise ServerConfigException("Certificate {} does not exist".format(cert_fname))

    try:
        with open(cert_fname) as f:
            verify_cacrt_content(f.read())
    except ValueError as e:
        raise ServerConfigException(
            'Invalid certificate {}: {}'.format(cert_fname, str(e)))


def verify_tls(crt, key):
    # type: (str, str) -> None
    verify_cacrt_content(crt)

    from OpenSSL import crypto, SSL
    try:
        _key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)
        _key.check()
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(
            'Invalid private key: {}'.format(str(e)))
    try:
        _crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt)
    except ValueError as e:
        raise ServerConfigException(
            'Invalid certificate key: {}'.format(str(e))
        )

    try:
        context = SSL.Context(SSL.TLSv1_METHOD)
        context.use_certificate(_crt)
        context.use_privatekey(_key)
        context.check_privatekey()
    except crypto.Error as e:
        logger.warning(
            'Private key and certificate do not match up: {}'.format(str(e)))


def verify_tls_files(cert_fname, pkey_fname):
    # type: (str, str) -> None
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

def to_pretty_timedelta(n):
    if n < datetime.timedelta(seconds=120):
        return str(n.seconds) + 's'
    if n < datetime.timedelta(minutes=120):
        return str(n.seconds // 60) + 'm'
    if n < datetime.timedelta(hours=48):
        return str(n.seconds // 3600) + 'h'
    if n < datetime.timedelta(days=14):
        return str(n.days) + 'd'
    if n < datetime.timedelta(days=7*12):
        return str(n.days // 7) + 'w'
    if n < datetime.timedelta(days=365*2):
        return str(n.days // 30) + 'M'
    return str(n.days // 365) + 'y'


def profile_method(skip_attribute=False):
    """
    Decorator for methods of the Module class. Logs the name of the given
    function f with the time it takes to execute it.
    """
    def outer(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            self = args[0]
            t = time.time()
            self.log.debug('Starting method {}.'.format(f.__name__))
            result = f(*args, **kwargs)
            duration = time.time() - t
            if not skip_attribute:
                wrapper._execution_duration = duration  # type: ignore
            self.log.debug('Method {} ran {:.3f} seconds.'.format(f.__name__, duration))
            return result
        return wrapper
    return outer
