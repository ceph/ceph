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


class DaemonStartException(Exception):
    """
    Special exception type we raise when the
    systemctl start command fails during daemon
    deployment. Necessary because the cephadm mgr module
    needs to handle this case differently than a failure
    earlier in the deploy process where no attempt was made
    to actually start the daemon
    """

    pass
