# Shared functions for working with networking (addresses, etc) across
# multiple ceph smb components.

from typing import Optional, Union

import ipaddress


IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]


def to_network(
    *,
    network: Optional[str] = None,
    address: Optional[str] = None,
) -> IPNetwork:
    """Parse and convert an ip address or ip network string into an ip network
    object such that a narrow set of result types are needed. Values may be
    ipv4 or ipv6.
    For ip address values the returned network will have a prefixlen of all
    "all ones".
    Only keyword arguments are supported.
    """
    if address and network:
        raise ValueError('only one of address or network may be given')
    if not (address or network):
        raise ValueError('one of address or network is required')

    if address:
        # verify that address is actually an address and doesn't contain a
        # network value (that would be accepted by ip_network).
        try:
            ipaddress.ip_address(address)
        except ValueError as err:
            raise ValueError(f'Cannot parse address {address}') from err

    addr = network if network else address
    try:
        assert addr
        nw = ipaddress.ip_network(addr)
    except ValueError as err:
        raise ValueError(f'Cannot parse network address {addr}') from err
    return nw
