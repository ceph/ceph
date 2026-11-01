"""
check that ganesha recalls NFSv4 delegations on conflicting CephFS access

The NFS client (a kernel mount from ganesha-client) holds a file open
under a delegation while a second CephFS client (ceph-fuse) accesses the
same file. The MDS revokes caps from ganesha's libcephfs client, which
asks ganesha to recall the delegation from the NFS client. Each scenario
checks that the conflicting access completes, that the NFS client returned
its delegation (DELEGRETURN in /proc/self/mountstats) and, where it
applies, that both clients see the same data. libcephfs forcibly unmounts
ganesha's client if a delegation is not returned in time, so ganesha's
CephFS session must survive all scenarios.

The export must expose the file system root (path and cmount_path "/"),
so that the same relative path names the same file on both mounts, and
ganesha must hand out delegations (see ganesha-reconf).

A scenario listed under known_failures is logged, not failed, when the
recall does not happen or the clients see different data; the task logs
any known failure that passed. Errors setting up a scenario, and the loss
of ganesha's CephFS session, always fail the task.

  - ganesha-deleg-recall:
      cluster_id: <nfs cluster id>
      nfs_client: client.0       # mounted by ganesha-client
      cephfs_client: client.1    # mounted by ceph-fuse
      fs_name: cephfs
      timeout: 60                # seconds, per conflicting access
      known_failures:            # scenario names
        - read-deleg-vs-write
"""

import json
import logging
import os
import time
from io import StringIO
from textwrap import dedent

from teuthology import misc
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)

class RecallFailure(Exception):
    """the delegation was not recalled or the clients disagree on the data"""

# hold <path> open until stdin is closed: 'r' reads it, 'w' writes
# <content> without syncing so that it stays dirty in the NFS client
HOLD_OPEN = dedent("""
    import sys
    path, mode, content, ready = sys.argv[1:]
    with open(path, mode) as f:
        if mode == 'w':
            f.write(content)
            f.flush()
        else:
            f.read()
        open(ready, 'w').close()
        sys.stdin.read()
    """)

class GaneshaDelegRecall(Task):
    def __init__(self, ctx, config):
        super(GaneshaDelegRecall, self).__init__(ctx, config)
        self.log = log
        self.holders = []

    def _remote(self, role):
        (remote,) = self.ctx.cluster.only(role).remotes.keys()
        return remote

    def _ceph(self, args):
        first_mon = misc.get_first_mon(self.ctx, None)
        proc = self._remote(first_mon).run(args=['ceph', *args], stdout=StringIO())
        return proc.stdout.getvalue()

    def _ganesha_sessions(self):
        sessions = json.loads(self._ceph(['tell', f'mds.{self.fs_name}:0',
                                          'session', 'ls', '--format=json']))
        prefix = f'nfs.{self.cluster_id}.'
        return {s['id'] for s in sessions
                if s.get('client_metadata', {}).get('entity_id', '').startswith(prefix)}

    def _delegreturns(self):
        stats = self.nfs_remote.read_file('/proc/self/mountstats').decode()
        mounted = False
        for line in stats.splitlines():
            if line.startswith('device '):
                mounted = f' mounted on {self.nfs_mnt} ' in line
            elif mounted and line.strip().startswith('DELEGRETURN:'):
                return int(line.split()[1])
        raise RuntimeError(f'no DELEGRETURN stats for {self.nfs_mnt}')

    def _hold_open(self, path, mode, content=''):
        """open path on the NFS mount and keep it open until _release()"""
        ready = f'/tmp/ganesha-deleg-recall.{os.path.basename(path)}.ready'
        self.nfs_remote.run(args=['rm', '-f', ready])
        proc = self.nfs_remote.run(args=['python3', '-c', HOLD_OPEN, path, mode,
                                         content, ready],
                                   stdin=run.PIPE, wait=False)
        self.holders.append(proc)
        for _ in range(self.timeout):
            if self.nfs_remote.run(args=['test', '-e', ready],
                                   check_status=False).exitstatus == 0:
                return proc
            if proc.finished:
                proc.wait()
            time.sleep(1)
        raise RuntimeError(f'timed out opening {path} on the NFS mount')

    def _release(self, proc):
        proc.stdin.close()
        proc.wait()
        self.holders.remove(proc)

    def _conflict(self, name, args):
        """run args on the CephFS client; the NFS client must return its delegation"""
        before = self._delegreturns()
        start = time.time()
        proc = self.cephfs_remote.run(args=['timeout', str(self.timeout), 'sudo', *args],
                                      stdout=StringIO(), check_status=False)
        elapsed = time.time() - start
        if proc.exitstatus != 0:
            raise RecallFailure(f'{name}: {args} on the CephFS client failed or timed '
                                f'out ({proc.exitstatus}) after {elapsed:.1f}s')
        returned = 0
        for _ in range(self.timeout):
            returned = self._delegreturns() - before
            if returned > 0:
                break
            time.sleep(1)
        log.info(f'{name}: {args} took {elapsed:.1f}s, {returned} delegation(s) returned')
        if returned == 0:
            raise RecallFailure(f'{name}: no delegation returned; was one granted?')
        return proc.stdout.getvalue()

    def _read_nfs(self, path):
        return self.nfs_remote.read_file(path).decode()

    def _test_read_deleg_vs_write(self, name):
        """a CephFS write recalls a read delegation; NFS sees the new data on reopen"""
        nfs_path = os.path.join(self.nfs_dir, name)
        self.nfs_remote.write_file(nfs_path, 'nfs')
        holder = self._hold_open(nfs_path, 'r')
        self._conflict(name, ['sh', '-c', f'echo -n cephfs > {self.cephfs_dir}/{name}'])
        self._release(holder)
        data = self._read_nfs(nfs_path)
        if data != 'cephfs':
            raise RecallFailure(f'{name}: NFS client read {data!r}, expected the CephFS write')

    def _test_write_deleg_vs_read(self, name):
        """a CephFS read recalls a write delegation; the NFS client's dirty data is flushed first"""
        nfs_path = os.path.join(self.nfs_dir, name)
        holder = self._hold_open(nfs_path, 'w', content='nfs-dirty')
        data = self._conflict(name, ['cat', f'{self.cephfs_dir}/{name}'])
        self._release(holder)
        if data != 'nfs-dirty':
            raise RecallFailure(f'{name}: CephFS client read {data!r}, expected the '
                                f'NFS client\'s unflushed write')

    def _test_read_deleg_vs_setattr(self, name):
        """a CephFS chmod recalls a read delegation"""
        nfs_path = os.path.join(self.nfs_dir, name)
        self.nfs_remote.write_file(nfs_path, 'nfs')
        holder = self._hold_open(nfs_path, 'r')
        self._conflict(name, ['chmod', '600', f'{self.cephfs_dir}/{name}'])
        self._release(holder)

    def begin(self):
        super(GaneshaDelegRecall, self).begin()

        self.cluster_id = self.config['cluster_id']
        self.fs_name = self.config.get('fs_name', 'cephfs')
        self.timeout = self.config.get('timeout', 60)
        nfs_client = self.config.get('nfs_client', 'client.0')
        cephfs_client = self.config.get('cephfs_client', 'client.1')

        self.nfs_remote = self._remote(nfs_client)
        self.cephfs_remote = self._remote(cephfs_client)
        test_dir = misc.get_testdir(self.ctx)
        # mount points of ganesha-client and ceph-fuse
        self.nfs_mnt = os.path.join(test_dir, f'mnt.{nfs_client.split(".")[1]}')
        cephfs_mnt = self.ctx.mounts[cephfs_client.split('.')[1]].hostfs_mntpt
        self.nfs_dir = os.path.join(self.nfs_mnt, 'deleg-recall')
        self.cephfs_dir = os.path.join(cephfs_mnt, 'deleg-recall')
        self.nfs_remote.run(args=['mkdir', '-p', self.nfs_dir])

        sessions = self._ganesha_sessions()
        log.info(f'ganesha CephFS sessions: {sessions}')
        if not sessions:
            raise RuntimeError('no ganesha CephFS session found')

        known = set(self.config.get('known_failures', []))
        scenarios = {
            'read-deleg-vs-write': self._test_read_deleg_vs_write,
            'write-deleg-vs-read': self._test_write_deleg_vs_read,
            'read-deleg-vs-setattr': self._test_read_deleg_vs_setattr,
        }
        unknown = known - scenarios.keys()
        if unknown:
            raise RuntimeError(f'unknown scenario(s) in known_failures: {unknown}')
        failed = []
        for name, scenario in scenarios.items():
            try:
                scenario(name)
            except RecallFailure as e:
                if name not in known:
                    raise
                log.warning(f'known failure: {e}')
                failed.append(name)
                for proc in list(self.holders):
                    self._release(proc)
                continue
            if name in known:
                log.info(f'{name}: passed, but is listed in known_failures')

        missing = sessions - self._ganesha_sessions()
        if missing:
            raise RuntimeError(f'ganesha CephFS session(s) {missing} went away')
        if failed:
            log.warning(f'known failures: {failed} (see the deleg-recall README)')

    def end(self):
        super(GaneshaDelegRecall, self).end()
        for proc in list(self.holders):
            self._release(proc)

task = GaneshaDelegRecall
