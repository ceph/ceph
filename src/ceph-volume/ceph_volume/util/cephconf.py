import logging
import os
import tempfile
from typing import Dict, List, Optional


logger = logging.getLogger(__name__)


class MkfsCephConfOverride:
    """
    Temporary ceph.conf override for a single mkfs call.

    In cephadm, `ceph-volume` runs in a container but mkfs may run on the host.
    If `/rootfs/tmp` exists, we mirror the temp file there so `/tmp/<file>` is
    visible from both sides. All temporary files are cleaned up on exits.
    """

    def __init__(
        self,
        base_conf_path: str,
        extra_conf: str,
        env: Optional[Dict[str, str]] = None,
        log: logging.Logger = logger,
    ) -> None:
        self.base_conf_path = base_conf_path
        self.extra_conf = extra_conf
        self.env = env
        self.log = log

        self._paths: List[str] = []
        self._ceph_conf_path: Optional[str] = None

    @property
    def ceph_conf_path(self) -> str:
        assert self._ceph_conf_path is not None
        return self._ceph_conf_path

    def __enter__(self) -> Dict[str, str]:
        base_contents = ''
        try:
            with open(self.base_conf_path, 'r', encoding='utf-8') as src:
                base_contents = src.read()
        except FileNotFoundError:
            base_contents = ''

        # Create a temp file under /tmp for this one mkfs call.
        fd, tmp_path = tempfile.mkstemp(prefix='ceph-volume-mkfs-', suffix='.conf', dir='/tmp')
        os.close(fd)
        self._paths.append(tmp_path)

        # In cephadm, /rootfs is the host root. If /rootfs/tmp exists, write a
        # copy there too so the host can read the same file at /tmp/<name>.
        basename = os.path.basename(tmp_path)
        if os.path.isdir('/rootfs/tmp'):
            self._paths.append(os.path.join('/rootfs/tmp', basename))

        contents = base_contents + self.extra_conf
        for path in self._paths:
            with open(path, 'w', encoding='utf-8') as dst:
                dst.write(contents)

        # Pass CEPH_CONF only to the mkfs subprocess, don't change global env.
        env = (self.env.copy() if self.env is not None else os.environ.copy())
        env['CEPH_CONF'] = tmp_path

        self._ceph_conf_path = tmp_path
        self.log.info('mkfs: using temporary ceph.conf override: %s', tmp_path)
        return env

    def __exit__(self, exc_type, exc, tb) -> None:
        for path in self._paths:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        self._paths = []
        self._ceph_conf_path = None
        return None


