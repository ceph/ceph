"""
create a CephFS export on an nfs-ganesha cluster

Exports the file system root, or one of the subvolumes created through
the ceph task (overrides.ceph.subvols), and waits for the nfs cluster's
ganesha daemon(s) to be running.

  - ganesha-export:
      cluster_id: <nfs cluster id>
      pseudo_path: /nfsganesha
      fs_name: cephfs
      mount_subvol_num: 0   # export the n-th created subvolume (optional)
      timeout: 300          # seconds to wait for the ganesha daemon(s)

mount_subvol_num can also be set through overrides.ganesha-export.
"""

import json
import logging
from io import StringIO

from teuthology.contextutil import safe_while
from teuthology.misc import deep_merge
from teuthology.task import Task
from teuthology import misc

log = logging.getLogger(__name__)

class GaneshaExport(Task):
    def __init__(self, ctx, config):
        super(GaneshaExport, self).__init__(ctx, config)
        self.log = log

    def _ceph(self, *args):
        proc = self.mon0_remote.run(args=['ceph', *args], stdout=StringIO(), wait=True)
        return proc.stdout.getvalue()

    def _export_path(self, fs_name, subvol_num):
        if subvol_num is None:
            return '/'
        subvols = getattr(self.ctx, 'created_subvols', {}).get(fs_name)
        if not subvols:
            raise RuntimeError(f'mount_subvol_num is set but no subvolume was created '
                               f'for {fs_name} (see overrides.ceph.subvols)')
        return subvols[int(subvol_num)]

    def _wait_for_daemons(self, service_name, timeout):
        with safe_while(sleep=10, tries=timeout // 10,
                        action=f'wait for {service_name} daemons') as proceed:
            while proceed():
                daemons = json.loads(self._ceph('orch', 'ps', '--service_name', service_name,
                                                '--refresh', '--format', 'json'))
                if daemons and all(d.get('status_desc') == 'running' for d in daemons):
                    return daemons
                log.info(f'waiting for {service_name} daemons: {daemons}')

    def begin(self):
        super(GaneshaExport, self).begin()

        config = self.config or {}
        overrides = self.ctx.config.get('overrides', {}).get('ganesha-export', {})
        deep_merge(config, overrides)
        log.info(f'config is {config}')

        cluster_id = config['cluster_id']
        pseudo_path = config['pseudo_path']
        fs_name = config.get('fs_name', 'cephfs')
        timeout = int(config.get('timeout', 300))

        first_mon = misc.get_first_mon(self.ctx, None)
        (self.mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()

        path = self._export_path(fs_name, config.get('mount_subvol_num'))
        log.info(f'exporting {fs_name}:{path} as {pseudo_path} on nfs cluster {cluster_id}')
        self._ceph('nfs', 'export', 'create', 'cephfs', '--cluster-id', cluster_id,
                   '--pseudo-path', pseudo_path, '--fsname', fs_name, '--path', path)
        log.info(self._ceph('nfs', 'export', 'info', cluster_id, pseudo_path))

        daemons = self._wait_for_daemons(f'nfs.{cluster_id}', timeout)
        log.info(f'nfs daemons: {daemons}')

task = GaneshaExport
