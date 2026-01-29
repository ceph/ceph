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
    """Parse and convert an IP address or IP network string into a network
    object so that calling code only needs to manage network objects (vs
    seperate address and network objects). The input values may be for either
    IPv4 or IPv6.
    If the input value is an address rather than a network the resulting
    network object will have a prefixlen value equal to the length of the
    maximum prefixlen - /32 for IPv4 and /128 for IPv6.
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
