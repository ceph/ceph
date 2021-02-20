import ipaddress


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
