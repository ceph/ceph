# constants.py - constant values used throughout the cephadm sources

# Default container images -----------------------------------------------------
DEFAULT_IMAGE = 'quay.ceph.io/ceph-ci/ceph:main'
DEFAULT_IMAGE_IS_MAIN = True
DEFAULT_IMAGE_RELEASE = 'tentacle'
DEFAULT_REGISTRY = 'quay.io'  # normalize unqualified digests to this
# ------------------------------------------------------------------------------

LATEST_STABLE_RELEASE = 'tentacle'
DATA_DIR = '/var/lib/ceph'
LOG_DIR = '/var/log/ceph'
LOCK_DIR = '/run/cephadm'
LOGROTATE_DIR = '/etc/logrotate.d'
SYSCTL_DIR = '/etc/sysctl.d'
UNIT_DIR = '/etc/systemd/system'
CEPH_CONF_DIR = 'config'
CEPH_CONF = 'ceph.conf'
CEPH_PUBKEY = 'ceph.pub'
CEPH_KEYRING = 'ceph.client.admin.keyring'
CEPH_DEFAULT_CONF = f'/etc/ceph/{CEPH_CONF}'
CEPH_DEFAULT_KEYRING = f'/etc/ceph/{CEPH_KEYRING}'
CEPH_DEFAULT_PUBKEY = f'/etc/ceph/{CEPH_PUBKEY}'
LOG_DIR_MODE = 0o770
DATA_DIR_MODE = 0o700
DEFAULT_MODE = 0o600
CONTAINER_INIT = True
MIN_PODMAN_VERSION = (2, 0, 2)
CGROUPS_SPLIT_PODMAN_VERSION = (2, 1, 0)
PIDS_LIMIT_UNLIMITED_PODMAN_VERSION = (3, 4, 1)
CUSTOM_PS1 = r'[ceph: \u@\h \W]\$ '
DEFAULT_TIMEOUT = None  # in seconds
DEFAULT_RETRY = 15
DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'
QUIET_LOG_LEVEL = 9  # DEBUG is 10, so using 9 to be lower level than DEBUG
NO_DEPRECATED = False
UID_NOBODY = 65534
GID_NOGROUP = 65534
DAEMON_FAILED_ERROR = 17
