# net_utils.py - Generic networking utility functions

import errno
import fcntl
import ipaddress
import logging
import os
import re
import socket
import struct

from typing import Dict, Tuple, List

from .context import CephadmContext
from .exceptions import Error, PortOccupiedError
from .file_utils import read_file

logger = logging.getLogger()


class EndPoint:
    """EndPoint representing an ip:port format"""

    def __init__(self, ip: str, port: int) -> None:
        self.ip = ip
        self.port = port

    def __str__(self) -> str:
        return f'{self.ip}:{self.port}'

    def __repr__(self) -> str:
        return f'{self.ip}:{self.port}'


def attempt_bind(ctx, s, address, port):
    # type: (CephadmContext, socket.socket, str, int) -> None
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((address, port))
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            msg = 'Cannot bind to IP %s port %d: %s' % (address, port, e)
            logger.warning(msg)
            raise PortOccupiedError(msg)
        else:
            raise e
    except Exception as e:
        raise Error(e)
    finally:
        s.close()


def port_in_use(ctx: CephadmContext, endpoint: EndPoint) -> bool:
    """Detect whether a port is in use on the local machine - IPv4 and IPv6"""
    logger.info('Verifying port %s ...' % str(endpoint))

    def _port_in_use(af: socket.AddressFamily, address: str) -> bool:
        try:
            s = socket.socket(af, socket.SOCK_STREAM)
            attempt_bind(ctx, s, address, endpoint.port)
        except PortOccupiedError:
            return True
        except OSError as e:
            if e.errno in (errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL):
                # Ignore EAFNOSUPPORT and EADDRNOTAVAIL as two interfaces are
                # being tested here and one might be intentionally be disabled.
                # In that case no error should be raised.
                return False
            else:
                raise e
        return False

    if endpoint.ip != '0.0.0.0' and endpoint.ip != '::':
        if is_ipv6(endpoint.ip):
            return _port_in_use(socket.AF_INET6, endpoint.ip)
        else:
            return _port_in_use(socket.AF_INET, endpoint.ip)

    return any(
        _port_in_use(af, address)
        for af, address in (
            (socket.AF_INET, '0.0.0.0'),
            (socket.AF_INET6, '::'),
        )
    )


def check_ip_port(ctx, ep):
    # type: (CephadmContext, EndPoint) -> None
    if not ctx.skip_ping_check:
        logger.info(f'Verifying IP {ep.ip} port {ep.port} ...')
        if is_ipv6(ep.ip):
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            ip = unwrap_ipv6(ep.ip)
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ip = ep.ip
        attempt_bind(ctx, s, ip, ep.port)


def check_subnet(subnets: str) -> Tuple[int, List[int], str]:
    """Determine whether the given string is a valid subnet

    :param subnets: subnet string, a single definition or comma separated list of CIDR subnets
    :returns: return code, IP version list of the subnets and msg describing any errors validation errors
    """

    rc = 0
    versions = set()
    errors = []
    subnet_list = subnets.split(',')
    for subnet in subnet_list:
        # ensure the format of the string is as expected address/netmask
        subnet = subnet.strip()
        if not re.search(r'\/\d+$', subnet):
            rc = 1
            errors.append(f'{subnet} is not in CIDR format (address/netmask)')
            continue
        try:
            v = ipaddress.ip_network(subnet).version
            versions.add(v)
        except ValueError as e:
            rc = 1
            errors.append(f'{subnet} invalid: {str(e)}')

    return rc, list(versions), ', '.join(errors)


def unwrap_ipv6(address):
    # type: (str) -> str
    if address.startswith('[') and address.endswith(']'):
        return address[1:-1]
    return address


def wrap_ipv6(address):
    # type: (str) -> str

    # We cannot assume it's already wrapped or even an IPv6 address if
    # it's already wrapped it'll not pass (like if it's a hostname) and trigger
    # the ValueError
    try:
        if ipaddress.ip_address(address).version == 6:
            return f'[{address}]'
    except ValueError:
        pass

    return address


def is_ipv6(address):
    # type: (str) -> bool
    address = unwrap_ipv6(address)
    try:
        return ipaddress.ip_address(address).version == 6
    except ValueError:
        logger.warning(
            'Address: {} is not a valid IP address'.format(address)
        )
        return False


def ip_in_subnets(ip_addr: str, subnets: str) -> bool:
    """Determine if the ip_addr belongs to any of the subnets list."""
    subnet_list = [x.strip() for x in subnets.split(',')]
    for subnet in subnet_list:
        ip_address = unwrap_ipv6(ip_addr) if is_ipv6(ip_addr) else ip_addr
        if ipaddress.ip_address(ip_address) in ipaddress.ip_network(subnet):
            return True
    return False


def get_ipv4_address(ifname):
    # type: (str) -> str
    def _extract(sock: socket.socket, offset: int) -> str:
        return socket.inet_ntop(
            socket.AF_INET,
            fcntl.ioctl(
                sock.fileno(),
                offset,
                struct.pack('256s', bytes(ifname[:15], 'utf-8')),
            )[20:24],
        )

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        addr = _extract(s, 35093)  # '0x8915' = SIOCGIFADDR
        dq_mask = _extract(s, 35099)  # 0x891b = SIOCGIFNETMASK
    except OSError:
        # interface does not have an ipv4 address
        return ''

    dec_mask = sum([bin(int(i)).count('1') for i in dq_mask.split('.')])
    return '{}/{}'.format(addr, dec_mask)


def get_ipv6_address(ifname):
    # type: (str) -> str
    if not os.path.exists('/proc/net/if_inet6'):
        return ''

    raw = read_file(['/proc/net/if_inet6'])
    data = raw.splitlines()
    # based on docs @ https://www.tldp.org/HOWTO/Linux+IPv6-HOWTO/ch11s04.html
    # field 0 is ipv6, field 2 is scope
    for iface_setting in data:
        field = iface_setting.split()
        if field[-1] == ifname:
            ipv6_raw = field[0]
            ipv6_fmt_pieces = []
            for p in range(0, len(field[0]), 4):
                ipv6_fmt_piece = ''.join(
                    [ipv6_raw[_p] for _p in range(p, p + 4)]
                )
                ipv6_fmt_pieces.append(ipv6_fmt_piece)
            ipv6_fmtd = ':'.join(ipv6_fmt_pieces)
            # apply naming rules using ipaddress module
            ipv6 = ipaddress.ip_address(ipv6_fmtd)
            return '{}/{}'.format(str(ipv6), int('0x{}'.format(field[2]), 16))
    return ''


def get_hostname():
    # type: () -> str
    return socket.gethostname()


def get_short_hostname():
    # type: () -> str
    return get_hostname().split('.', 1)[0]


def get_fqdn():
    # type: () -> str
    return socket.getfqdn() or socket.gethostname()


def get_ip_addresses(hostname: str) -> Tuple[List[str], List[str]]:
    items = socket.getaddrinfo(
        hostname, None, flags=socket.AI_CANONNAME, type=socket.SOCK_STREAM
    )
    ipv4_addresses = [i[4][0] for i in items if i[0] == socket.AF_INET]
    ipv6_addresses = [i[4][0] for i in items if i[0] == socket.AF_INET6]
    return ipv4_addresses, ipv6_addresses


def parse_mon_addrv(addrv_arg: str) -> List[EndPoint]:
    """Parse mon-addrv param into a list of mon end points."""
    r = re.compile(r':(\d+)$')
    addrv_args = []
    addr_arg = addrv_arg
    if addr_arg[0] != '[' or addr_arg[-1] != ']':
        raise Error(f'--mon-addrv value {addr_arg} must use square brackets')

    for addr in addr_arg[1:-1].split(','):
        hasport = r.findall(addr)
        if not hasport:
            raise Error(
                f'--mon-addrv value {addr_arg} must include port number'
            )
        port_str = hasport[0]
        addr = re.sub(r'^v\d+:', '', addr)  # strip off v1: or v2: prefix
        base_ip = addr[: -(len(port_str)) - 1]
        addrv_args.append(EndPoint(base_ip, int(port_str)))

    return addrv_args


def parse_mon_ip(mon_ip: str) -> List[EndPoint]:
    """Parse mon-ip param into a list of mon end points."""
    r = re.compile(r':(\d+)$')
    addrv_args = []
    hasport = r.findall(mon_ip)
    if hasport:
        port_str = hasport[0]
        base_ip = mon_ip[: -(len(port_str)) - 1]
        addrv_args.append(EndPoint(base_ip, int(port_str)))
    else:
        # No port provided: use fixed ports for ceph monitor
        addrv_args.append(EndPoint(mon_ip, 3300))
        addrv_args.append(EndPoint(mon_ip, 6789))

    return addrv_args


def build_addrv_params(addrv: List[EndPoint]) -> str:
    """Convert mon end-points (ip:port) into the format: [v[1|2]:ip:port1]"""
    if len(addrv) > 2:
        raise Error(
            'Detected a local mon-addrv list with more than 2 entries.'
        )
    port_to_ver: Dict[int, str] = {6789: 'v1', 3300: 'v2'}
    addr_arg_list: List[str] = []
    for ep in addrv:
        if ep.port in port_to_ver:
            ver = port_to_ver[ep.port]
        else:
            ver = 'v2'  # default mon protocol version if port is not provided
            logger.warning(f'Using msgr2 protocol for unrecognized port {ep}')
        addr_arg_list.append(f'{ver}:{ep.ip}:{ep.port}')

    addr_arg = '[{0}]'.format(','.join(addr_arg_list))
    return addr_arg
