# exceptions.py - cephadm specific exception types


class Error(Exception):
    pass


class ClusterAlreadyExists(Exception):
    pass


class TimeoutExpired(Error):
    pass


class UnauthorizedRegistryError(Error):
    pass


class PortOccupiedError(Error):
    pass
