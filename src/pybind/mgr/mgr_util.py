import os

if 'UNITTEST' in os.environ:
    import tests

import bcrypt
import cephfs
import contextlib
import datetime
import errno
import socket
import time
import logging
import sys
from ipaddress import ip_address
from threading import Lock, Condition, Event
from typing import no_type_check, NewType
import urllib
from functools import wraps
if sys.version_info >= (3, 3):
    from threading import Timer
else:
    from threading import _Timer as Timer

from typing import Tuple, Any, Callable, Optional, Dict, TYPE_CHECKING, TypeVar, List, Iterable, Generator, Generic, Iterator

from ceph.deployment.utils import wrap_ipv6

T = TypeVar('T')

if TYPE_CHECKING:
    from mgr_module import MgrModule

ConfEntity = NewType('ConfEntity', str)

Module_T = TypeVar('Module_T', bound="MgrModule")

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


class PortAlreadyInUse(Exception):
    pass


class CephfsConnectionException(Exception):
    def __init__(self, error_code: int, error_message: str):
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self) -> Tuple[int, str, str]:
        return self.errno, "", self.error_str

    def __str__(self) -> str:
        return "{0} ({1})".format(self.errno, self.error_str)

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
            logger.error("task exception: %s", e)
            raise

@contextlib.contextmanager
def lock_timeout_log(lock: Lock, timeout: int = 5) -> Iterator[None]:
    start = time.time()
    WARN_AFTER = 30
    warned = False
    while True:
        logger.debug("locking {} with {} timeout".format(lock, timeout))
        if lock.acquire(timeout=timeout):
            logger.debug("locked {}".format(lock))
            yield
            lock.release()
            break
        now = time.time()
        if not warned and now - start > WARN_AFTER:
            logger.info("possible deadlock acquiring {}".format(lock))
            warned = True


class CephfsConnectionPool(object):
    class Connection(object):
        def __init__(self, mgr: Module_T, fs_name: str):
            self.fs: Optional["cephfs.LibCephFS"] = None
            self.mgr = mgr
            self.fs_name = fs_name
            self.ops_in_progress = 0
            self.last_used = time.time()
            self.fs_id = self.get_fs_id()

        def get_fs_id(self) -> int:
            fs_map = self.mgr.get('fs_map')
            for fs in fs_map['filesystems']:
                if fs['mdsmap']['fs_name'] == self.fs_name:
                    return fs['id']
            raise CephfsConnectionException(
                -errno.ENOENT, "FS '{0}' not found".format(self.fs_name))

        def get_fs_handle(self) -> "cephfs.LibCephFS":
            self.last_used = time.time()
            self.ops_in_progress += 1
            return self.fs

        def put_fs_handle(self, notify: Callable) -> None:
            assert self.ops_in_progress > 0
            self.ops_in_progress -= 1
            if self.ops_in_progress == 0:
                notify()

        def del_fs_handle(self, waiter: Optional[Callable]) -> None:
            if waiter:
                while self.ops_in_progress != 0:
                    waiter()
            if self.is_connection_valid():
                self.disconnect()
            else:
                self.abort()

        def is_connection_valid(self) -> bool:
            fs_id = None
            try:
                fs_id = self.get_fs_id()
            except:
                # the filesystem does not exist now -- connection is not valid.
                pass
            logger.debug("self.fs_id={0}, fs_id={1}".format(self.fs_id, fs_id))
            return self.fs_id == fs_id

        def is_connection_idle(self, timeout: float) -> bool:
            return (self.ops_in_progress == 0 and ((time.time() - self.last_used) >= timeout))

        def connect(self) -> None:
            assert self.ops_in_progress == 0
            logger.debug("Connecting to cephfs '{0}'".format(self.fs_name))
            self.fs = cephfs.LibCephFS(rados_inst=self.mgr.rados)
            logger.debug("Setting user ID and group ID of CephFS mount as root...")
            self.fs.conf_set("client_mount_uid", "0")
            self.fs.conf_set("client_mount_gid", "0")
            self.fs.conf_set("client_check_pool_perm", "false")
            self.fs.conf_set("client_quota", "false")
            logger.debug("CephFS initializing...")
            self.fs.init()
            logger.debug("CephFS mounting...")
            self.fs.mount(filesystem_name=self.fs_name.encode('utf-8'))
            logger.debug("Connection to cephfs '{0}' complete".format(self.fs_name))
            self.mgr._ceph_register_client(None, self.fs.get_addrs(), False)

        def disconnect(self) -> None:
            try:
                assert self.fs
                assert self.ops_in_progress == 0
                logger.info("disconnecting from cephfs '{0}'".format(self.fs_name))
                addrs = self.fs.get_addrs()
                self.fs.shutdown()
                self.mgr._ceph_unregister_client(None, addrs)
                self.fs = None
            except Exception as e:
                logger.debug("disconnect: ({0})".format(e))
                raise

        def abort(self) -> None:
            assert self.fs
            assert self.ops_in_progress == 0
            logger.info("aborting connection from cephfs '{0}'".format(self.fs_name))
            self.fs.abort_conn()
            logger.info("abort done from cephfs '{0}'".format(self.fs_name))
            self.fs = None

    # TODO: make this configurable
    TIMER_TASK_RUN_INTERVAL = 30.0   # seconds
    CONNECTION_IDLE_INTERVAL = 60.0  # seconds
    MAX_CONCURRENT_CONNECTIONS = 5   # max number of concurrent connections per volume

    def __init__(self, mgr: Module_T):
        self.mgr = mgr
        self.connections: Dict[str, List[CephfsConnectionPool.Connection]] = {}
        self.lock = Lock()
        self.cond = Condition(self.lock)
        self.timer_task = RTimer(CephfsConnectionPool.TIMER_TASK_RUN_INTERVAL,
                                 self.cleanup_connections)
        self.timer_task.start()

    def cleanup_connections(self) -> None:
        with self.lock:
            logger.info("scanning for idle connections..")
            idle_conns = []
            for fs_name, connections in self.connections.items():
                logger.debug(f'fs_name ({fs_name}) connections ({connections})')
                for connection in connections:
                    if connection.is_connection_idle(CephfsConnectionPool.CONNECTION_IDLE_INTERVAL):
                        idle_conns.append((fs_name, connection))
            logger.info(f'cleaning up connections: {idle_conns}')
            for idle_conn in idle_conns:
                self._del_connection(idle_conn[0], idle_conn[1])

    def get_fs_handle(self, fs_name: str) -> "cephfs.LibCephFS":
        with self.lock:
            try:
                min_shared = 0
                shared_connection = None
                connections = self.connections.setdefault(fs_name, [])
                logger.debug(f'[get] volume: ({fs_name}) connection: ({connections})')
                if connections:
                    min_shared = connections[0].ops_in_progress
                    shared_connection = connections[0]
                for connection in list(connections):
                    logger.debug(f'[get] connection: {connection} usage: {connection.ops_in_progress}')
                    if connection.ops_in_progress == 0:
                        if connection.is_connection_valid():
                            logger.debug(f'[get] connection ({connection}) can be reused')
                            return connection.get_fs_handle()
                        else:
                            # filesystem id changed beneath us (or the filesystem does not exist).
                            # this is possible if the filesystem got removed (and recreated with
                            # same name) via "ceph fs rm/new" mon command.
                            logger.warning(f'[get] filesystem id changed for volume ({fs_name}), disconnecting ({connection})')
                            # note -- this will mutate @connections too
                            self._del_connection(fs_name, connection)
                    else:
                        if connection.ops_in_progress < min_shared:
                            min_shared = connection.ops_in_progress
                            shared_connection = connection
                # when we end up here, there are no "free" connections. so either spin up a new
                # one or share it.
                if len(connections) < CephfsConnectionPool.MAX_CONCURRENT_CONNECTIONS:
                    logger.debug('[get] spawning new connection since no connection is unused and we still have room for more')
                    connection = CephfsConnectionPool.Connection(self.mgr, fs_name)
                    connection.connect()
                    self.connections[fs_name].append(connection)
                    return connection.get_fs_handle()
                else:
                    assert shared_connection is not None
                    logger.debug(f'[get] using shared connection ({shared_connection})')
                    return shared_connection.get_fs_handle()
            except cephfs.Error as e:
                # try to provide a better error string if possible
                if e.args[0] == errno.ENOENT:
                    raise CephfsConnectionException(
                        -errno.ENOENT, "FS '{0}' not found".format(fs_name))
                raise CephfsConnectionException(-e.args[0], e.args[1])

    def put_fs_handle(self, fs_name: str, fs_handle: cephfs.LibCephFS) -> None:
        with self.lock:
            connections = self.connections.get(fs_name, [])
            for connection in connections:
                if connection.fs == fs_handle:
                    logger.debug(f'[put] connection: {connection} usage: {connection.ops_in_progress}')
                    connection.put_fs_handle(notify=lambda: self.cond.notifyAll())

    def _del_connection(self, fs_name: str, connection: Connection, wait: bool = False) -> None:
        self.connections[fs_name].remove(connection)
        connection.del_fs_handle(waiter=None if not wait else lambda: self.cond.wait())

    def _del_connections(self, fs_name: str, wait: bool = False) -> None:
        for connection in list(self.connections.get(fs_name, [])):
            self._del_connection(fs_name, connection, wait)

    def del_connections(self, fs_name: str, wait: bool = False) -> None:
        with self.lock:
            self._del_connections(fs_name, wait)

    def del_all_connections(self) -> None:
        with self.lock:
            for fs_name in list(self.connections.keys()):
                logger.info("waiting for pending ops for '{}'".format(fs_name))
                self._del_connections(fs_name, wait=True)
                logger.info("pending ops completed for '{}'".format(fs_name))
            # no new connections should have been initialized since its
            # guarded on shutdown.
            assert len(self.connections) == 0


class CephfsClient(Generic[Module_T]):
    def __init__(self, mgr: Module_T):
        self.mgr = mgr
        self.connection_pool = CephfsConnectionPool(self.mgr)

    def shutdown(self) -> None:
        logger.info("shutting down")
        # second, delete all libcephfs handles from connection pool
        self.connection_pool.del_all_connections()

    def get_fs(self, fs_name: str) -> Optional["cephfs.LibCephFS"]:
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs
        return None

    def get_mds_names(self, fs_name: str) -> List[str]:
        fs = self.get_fs(fs_name)
        if fs is None:
            return []
        return [mds['name'] for mds in fs['mdsmap']['info'].values()]

    def get_metadata_pool(self, fs_name: str) -> Optional[str]:
        fs = self.get_fs(fs_name)
        if fs:
            return fs['mdsmap']['metadata_pool']
        return None

    def get_all_filesystems(self) -> List[str]:
        fs_list: List[str] = []
        fs_map = self.mgr.get('fs_map')
        if fs_map['filesystems']:
            for fs in fs_map['filesystems']:
                fs_list.append(fs['mdsmap']['fs_name'])
        return fs_list



@contextlib.contextmanager
def open_filesystem(fsc: CephfsClient, fs_name: str) -> Generator["cephfs.LibCephFS", None, None]:
    """
    Open a volume with shared access.
    This API is to be used as a context manager.

    :param fsc: cephfs client instance
    :param fs_name: fs name
    :return: yields a fs handle (ceph filesystem handle)
    """
    fs_handle = fsc.connection_pool.get_fs_handle(fs_name)
    try:
        yield fs_handle
    finally:
        fsc.connection_pool.put_fs_handle(fs_name, fs_handle)


def colorize(msg: str, color: int, dark: bool = False) -> str:
    """
    Decorate `msg` with escape sequences to give the requested color
    """
    return (COLOR_DARK_SEQ if dark else COLOR_SEQ) % (30 + color) \
        + msg + RESET_SEQ


def bold(msg: str) -> str:
    """
    Decorate `msg` with escape sequences to make it appear bold
    """
    return BOLD_SEQ + msg + RESET_SEQ


def format_units(n: int, width: int, colored: bool, decimal: bool) -> str:
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
            + bold(colorize(formatted[-1], YELLOW, False))
    else:
        return formatted


def format_dimless(n: int, width: int, colored: bool = False) -> str:
    return format_units(n, width, colored, decimal=True)


def format_bytes(n: int, width: int, colored: bool = False) -> str:
    return format_units(n, width, colored, decimal=False)


def test_port_allocation(addr: str, port: int) -> None:
    """Checks if the port is available
    :raises PortAlreadyInUse: in case port is already in use
    :raises Exception: any generic error other than port already in use
    If no exception is raised, the port can be assumed available
    """
    try:
        ip_version = ip_address(addr).version
        addr_family = socket.AF_INET if ip_version == 4 else socket.AF_INET6
        sock = socket.socket(addr_family, socket.SOCK_STREAM)
        sock.bind((addr, port))
        sock.close()
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            raise PortAlreadyInUse
        else:
            raise e


def merge_dicts(*args: Dict[T, Any]) -> Dict[T, Any]:
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
    def is_ipv6_enabled() -> bool:
        try:
            sock = socket.socket(socket.AF_INET6)
            with contextlib.closing(sock):
                sock.bind(("::1", 0))
                return True
        except (AttributeError, socket.error):
            return False

    try:
        return get_default_addr.result  # type: ignore
    except AttributeError:
        result = '::' if is_ipv6_enabled() else '0.0.0.0'
        get_default_addr.result = result  # type: ignore
        return result


def build_url(host: str, scheme: Optional[str] = None, port: Optional[int] = None, path: str = '') -> str:
    """
    Build a valid URL. IPv6 addresses specified in host will be enclosed in brackets
    automatically.

    >>> build_url('example.com', 'https', 443)
    'https://example.com:443'

    >>> build_url(host='example.com', port=443)
    '//example.com:443'

    >>> build_url('fce:9af7:a667:7286:4917:b8d3:34df:8373', port=80, scheme='http')
    'http://[fce:9af7:a667:7286:4917:b8d3:34df:8373]:80'

    >>> build_url('example.com', 'https', 443, path='/metrics')
    'https://example.com:443/metrics'


    :param scheme: The scheme, e.g. http, https or ftp.
    :type scheme: str
    :param host: Consisting of either a registered name (including but not limited to
                 a hostname) or an IP address.
    :type host: str
    :type port: int
    :rtype: str
    """
    netloc = wrap_ipv6(host)
    if port:
        netloc += ':{}'.format(port)
    pr = urllib.parse.ParseResult(
        scheme=scheme if scheme else '',
        netloc=netloc,
        path=path,
        params='',
        query='',
        fragment='')
    return pr.geturl()


class ServerConfigException(Exception):
    pass


def create_self_signed_cert(organisation: str = 'Ceph',
                            common_name: str = 'mgr',
                            dname: Optional[Dict[str, str]] = None) -> Tuple[str, str]:
    """Returns self-signed PEM certificates valid for 10 years.

    The optional dname parameter provides complete control of the cert/key
    creation by supporting all valid RDNs via a dictionary. However, if dname
    is not provided the default O and CN settings will be applied.

    :param organisation: String representing the Organisation(O) RDN (default='Ceph')
    :param common_name: String representing the Common Name(CN) RDN (default='mgr')
    :param dname: Optional dictionary containing RDNs to use for crt/key generation 

    :return: ssl crt and key in utf-8 format

    :raises ValueError: if the dname parameter received contains invalid RDNs

    """

    from OpenSSL import crypto
    from uuid import uuid4

    # RDN = Relative Distinguished Name
    valid_RDN_list = ['C', 'ST', 'L', 'O', 'OU', 'CN', 'emailAddress']

    # create a key pair
    pkey = crypto.PKey()
    pkey.generate_key(crypto.TYPE_RSA, 2048)

    # Create a "subject" object
    req = crypto.X509Req()
    subj = req.get_subject()

    if dname:
        # dname received, so check it contains valid RDNs
        if not all(field in valid_RDN_list for field in dname):
            raise ValueError("Invalid DNAME received. Valid DNAME fields are {}".format(', '.join(valid_RDN_list)))
    else:
        dname = {"O": organisation, "CN": common_name}

    # populate the subject with the dname settings
    for k, v in dname.items():
        setattr(subj, k, v)

    # create a self-signed cert
    cert = crypto.X509()
    cert.set_subject(req.get_subject())
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
        crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
        x509 = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
        if x509.has_expired():
            org, cn = get_cert_issuer_info(crt)
            no_after = x509.get_notAfter()
            end_date = None
            if no_after is not None:
                end_date = datetime.datetime.strptime(no_after.decode('ascii'), '%Y%m%d%H%M%SZ')
            msg = f'Certificate issued by "{org}/{cn}" expired on {end_date}'
            logger.warning(msg)
            raise ServerConfigException(msg)
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(f'Invalid certificate: {e}')


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

def get_cert_issuer_info(crt: str) -> Tuple[Optional[str],Optional[str]]:
    """Basic validation of a ca cert"""

    from OpenSSL import crypto, SSL
    try:
        crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
        (org_name, cn) = (None, None)
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
        components = cert.get_issuer().get_components()
        for c in components:
            if c[0].decode() == 'O':  # org comp
                org_name = c[1].decode()
            elif c[0].decode() == 'CN':  # common name comp
                cn = c[1].decode()
        return (org_name, cn)
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(f'Invalid certificate key: {e}')

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
        crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
        _crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
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
        logger.warning('Private key and certificate do not match up: {}'.format(str(e)))
    except SSL.Error as e:
        raise ServerConfigException(f'Invalid cert/key pair: {e}')



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


def get_most_recent_rate(rates: Optional[List[Tuple[float, float]]]) -> float:
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

def get_time_series_rates(data: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
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
    return [(data2[0], _derivative(data1, data2) if data1 is not None else 0.0) for data1, data2 in
            _pairwise(data)]

def name_to_config_section(name: str) -> ConfEntity:
    """
    Map from daemon names to ceph entity names (as seen in config)
    """
    daemon_type = name.split('.', 1)[0]
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', 'crash', 'iscsi']:
        return ConfEntity('client.' + name)
    elif daemon_type in ['mon', 'osd', 'mds', 'mgr', 'client']:
        return ConfEntity(name)
    else:
        return ConfEntity('mon')


def _filter_time_series(data: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
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


def _derivative(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
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


def _pairwise(iterable: Iterable[T]) -> Generator[Tuple[Optional[T], T], None, None]:
    it = iter(iterable)
    a = next(it, None)

    for b in it:
        yield (a, b)
        a = b


def to_pretty_timedelta(n: datetime.timedelta) -> str:
    if n < datetime.timedelta(seconds=120):
        return str(int(n.total_seconds())) + 's'
    if n < datetime.timedelta(minutes=120):
        return str(int(n.total_seconds()) // 60) + 'm'
    if n < datetime.timedelta(hours=48):
        return str(int(n.total_seconds()) // 3600) + 'h'
    if n < datetime.timedelta(days=14):
        return str(int(n.total_seconds()) // (3600*24)) + 'd'
    if n < datetime.timedelta(days=7*12):
        return str(int(n.total_seconds()) // (3600*24*7)) + 'w'
    if n < datetime.timedelta(days=365*2):
        return str(int(n.total_seconds()) // (3600*24*30)) + 'M'
    return str(int(n.total_seconds()) // (3600*24*365)) + 'y'


def profile_method(skip_attribute: bool = False) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for methods of the Module class. Logs the name of the given
    function f with the time it takes to execute it.
    """
    def outer(f: Callable[..., T]) -> Callable[..., T]:
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> T:
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


def password_hash(password: Optional[str], salt_password: Optional[str] = None) -> Optional[str]:
    if not password:
        return None
    if not salt_password:
        salt = bcrypt.gensalt()
    else:
        salt = salt_password.encode('utf8')
    return bcrypt.hashpw(password.encode('utf8'), salt).decode('utf8')
