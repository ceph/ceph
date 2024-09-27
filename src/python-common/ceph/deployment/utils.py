import ipaddress
import socket
from typing import Tuple, Optional
from urllib.parse import urlparse


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
            return f"[{address}]"
    except ValueError:
        pass

    return address


def is_ipv6(address):
    # type: (str) -> bool
    address = unwrap_ipv6(address)
    try:
        return ipaddress.ip_address(address).version == 6
    except ValueError:
        return False


def valid_addr(addr: str) -> Tuple[bool, str]:
    """check that an address string is valid
    Valid in this case means that a name is resolvable, or the
    IP address string is a correctly formed IPv4 or IPv6 address,
    with or without a port

    Args:
        addr (str): address

    Returns:
        Tuple[bool, str]: Validity of the address, either
                          True, address type (IPv4[:Port], IPv6[:Port], Name[:Port])
                          False, <error description>
    """

    def _dns_lookup(addr: str, port: Optional[int]) -> Tuple[bool, str]:
        try:
            socket.getaddrinfo(addr, None)
        except socket.gaierror:
            # not resolvable
            return False, 'DNS lookup failed'
        return True, 'Name:Port' if port else 'Name'

    def _ip_lookup(addr: str, port: Optional[int]) -> Tuple[bool, str]:
        unwrapped = unwrap_ipv6(addr)
        try:
            ip_addr = ipaddress.ip_address(unwrapped)
        except ValueError:
            return False, 'Invalid IP v4 or v6 address format'
        return True, f'IPv{ip_addr.version}:Port' if port else f'IPv{ip_addr.version}'

    dots = addr.count('.')
    colons = addr.count(':')
    addr_as_url = f'http://{addr}'

    if addr.startswith('[') and dots:
        return False, "IPv4 address wrapped in brackets is invalid"

    try:
        res = urlparse(addr_as_url)
    except ValueError as e:
        if str(e) == 'Invalid IPv6 URL':
            return False, 'Address has incorrect/incomplete use of enclosing brackets'
        return False, f'Unknown urlparse error {str(e)} for {addr_as_url}'

    addr = res.netloc
    port = None
    try:
        port = res.port
        if port:
            addr = addr[:-len(f':{port}')]
    except ValueError:
        if colons == 1:
            return False, 'Port must be numeric'
        elif ']:' in addr:
            return False, 'Port must be numeric'

    # catch partial address like 10.8 which would be valid IPaddress schemes
    # but are classed as invalid here since they're not usable
    if dots and addr[0].isdigit() and dots != 3:
        return False, 'Invalid partial IPv4 address'

    if addr[0].isalpha() and '.' in addr:
        return _dns_lookup(addr, port)
    return _ip_lookup(addr, port)
