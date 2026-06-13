# Shared constant values that apply across all ceph components that manage
# smb services (mgr module, cephadm mgr module, cephadm binary, etc)


# Generic/common names
SMB = 'smb'
CTDB = 'ctdb'


# Feature names
# (please keep sorted)
CEPHFS_PROXY = 'cephfs-proxy'
CLUSTERED = 'clustered'
DOMAIN = 'domain'
KEYBRIDGE = 'keybridge'
REMOTE_CONTROL = 'remote-control'
REMOTE_CONTROL_LOCAL = 'remote-control-local'
SMBMETRICS = 'smbmetrics'


# Features are optional components that can be deployed in a suite of smb
# related containers. It may run as a separate sidecar or side-effect the
# configuration of another component.
FEATURES = {
    CEPHFS_PROXY,
    CLUSTERED,
    DOMAIN,
    KEYBRIDGE,
    REMOTE_CONTROL,
    REMOTE_CONTROL_LOCAL,
}

# Services are components that listen on a "public" network port, to expose
# network services to remote clients.
SERVICES = {
    CTDB,
    REMOTE_CONTROL,
    SMB,
    SMBMETRICS,
}


# Default port values
SMB_PORT = 445
SMBMETRICS_PORT = 9922
CTDB_PORT = 4379
REMOTE_CONTROL_PORT = 54445

DEFAULT_PORTS = {
    SMB: SMB_PORT,
    SMBMETRICS: SMBMETRICS_PORT,
    CTDB: CTDB_PORT,
    REMOTE_CONTROL: REMOTE_CONTROL_PORT,
}


# Debugging levels (names/translation)
DEBUG_LEVEL_TIERS = [
    ("ERROR", 0, 0),
    ("WARNING", 1, 2),
    ("NOTICE", 3, 4),
    ("INFO", 5, 8),
    ("DEBUG", 9, 10),
]
DEBUG_LEVEL_TERMS = {t[0] for t in DEBUG_LEVEL_TIERS}

# Maximum value for iops_limit
IOPS_LIMIT_MAX = 1_000_000

# Maximum value for bandwidth limit (1 << 40 = 1 TB)
BYTES_LIMIT_MAX = 1 << 40

# Minimum value for burst multiplier
BURST_MULT_MIN = 10

# Maximum value for burst multiplier
BURST_MULT_MAX = 100
