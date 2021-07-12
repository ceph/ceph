try:
    from . import version
    __version__ = f'{version.CEPH_GIT_NICE_VER} ({version.CEPH_GIT_VER}) ' \
                  f'{version.CEPH_RELEASE_NAME} ({version.CEPH_RELEASE_TYPE})'
    __release__ = {version.CEPH_RELEASE_NAME}
except ImportError:
    __version__ = 'no_version'
    __release__ = "no_release"
