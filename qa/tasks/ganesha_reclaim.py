"""
check that an MDS rank is released from a ganesha session that missed reconnect

Ganesha's libcephfs client sets a session timeout (client metadata
"timeout"). When such a client does not reconnect to a restarted MDS
rank in time, the MDS keeps its session instead of evicting it, and the
rank waits in up:clientreplay until the session is reclaimed by a new
ganesha instance (same uuid), evicted by an admin or autoclosed.

The task runs one scenario. It freezes ganesha (SIGSTOP), fails rank 0,
checks that the new MDS holds the rank in up:clientreplay with ganesha's
session kept, and then ends the hold:

  restart  ganesha is killed and restarted: the new instance reclaims
           the session
  evict    the session is evicted with "client evict"; ganesha is then
           restarted, as its client is blocklisted
  late     ganesha is resumed and the NFS client writes a file: ganesha
           missed the reconnect but is still running

Rank 0 has to go active within rank_timeout seconds and the NFS client
has to be able to write and read a file afterwards. If not, the task
evicts the kept session and restarts ganesha before failing, so that the
teardown finds a working rank 0 and ganesha. The nfs cluster must have a
single ganesha daemon.

  - ganesha-reclaim:
      cluster_id: <nfs cluster id>
      scenario: restart          # restart, evict or late
      nfs_client: client.0       # mounted by ganesha-client
      fs_name: cephfs
      rank_timeout: 120          # seconds for rank 0 to go active
      observe_timeout: 600       # late: seconds to keep watching rank 0
      io_timeout: 300            # seconds for NFS I/O (covers grace)
      hold_check: 15             # seconds rank 0 must stay held
      timeout: 300               # seconds for failovers and restarts
"""

import json
import logging
import os
import time
from io import StringIO

from teuthology import misc
from teuthology.task import Task

from tasks.ganesha_thrash import GaneshaDaemon, _host_remote, _nfs_daemons, _nfs_port

log = logging.getLogger(__name__)

class ReclaimFailure(Exception):
    """rank 0 was not released in time or NFS I/O failed afterwards"""

class GaneshaReclaim(Task):
    # for overrides.ganesha-reclaim
    name = 'ganesha-reclaim'

    def __init__(self, ctx, config):
        super(GaneshaReclaim, self).__init__(ctx, config)
        self.log = log
        self.frozen = None      # pid of the stopped ganesha.nfsd
        self.failed_at = None   # when rank 0 was failed
        self.pending_io = []    # NFS I/O left running by a failed scenario

    def _ceph(self, args):
        proc = self.mon0_remote.run(args=['ceph', *args], stdout=StringIO())
        return proc.stdout.getvalue()

    def _wait(self, cond, timeout, what):
        start = time.time()
        while time.time() - start < timeout:
            if cond():
                return
            time.sleep(2)
        raise RuntimeError(f'timed out after {timeout}s waiting for {what}')

    def _fs(self):
        return json.loads(self._ceph(['fs', 'get', self.fs_name, '--format=json']))['mdsmap']

    def _rank0(self):
        """(gid, state) of the MDS holding rank 0, (None, None) if none"""
        for info in self._fs()['info'].values():
            if info['rank'] == 0:
                return info['gid'], info['state']
        return None, None

    def _wait_for_active(self, start, timeout):
        """seconds from start until rank 0 is up:active, None if not within timeout"""
        while True:
            _, state = self._rank0()
            elapsed = time.time() - start
            if state == 'up:active':
                return elapsed
            if elapsed >= timeout:
                return None
            time.sleep(2)

    def _wait_for_standby(self):
        def standby():
            return bool(json.loads(self._ceph(['fs', 'dump', '--format=json']))['standbys'])
        self._wait(standby, self.timeout, 'a standby MDS')

    def _ganesha_sessions(self):
        """id -> client metadata of ganesha's CephFS sessions on rank 0"""
        sessions = json.loads(self._ceph(['tell', f'mds.{self.fs_name}:0',
                                          'session', 'ls', '--format=json']))
        prefix = f'nfs.{self.cluster_id}.'
        return {s['id']: s.get('client_metadata', {}) for s in sessions
                if s.get('client_metadata', {}).get('entity_id', '').startswith(prefix)}

    def _evict(self, session_id):
        self._ceph(['tell', f'mds.{self.fs_name}:0', 'client', 'evict', f'id={session_id}'])

    def _freeze_ganesha(self):
        remote = self.ganesha.remote
        proc = remote.run(args=['pgrep', '-x', 'ganesha.nfsd'], stdout=StringIO(),
                          check_status=False)
        pids = proc.stdout.getvalue().split()
        if len(pids) != 1:
            raise RuntimeError(f'expected one ganesha.nfsd on {remote.shortname}, found {pids}')
        remote.run(args=['sudo', 'kill', '-STOP', pids[0]])
        self.frozen = pids[0]

    def _thaw_ganesha(self):
        self.ganesha.remote.run(args=['sudo', 'kill', '-CONT', self.frozen])
        self.frozen = None

    def _restart_ganesha(self):
        """replace the ganesha instance, killing it even if it is stopped"""
        self.ganesha.kill()
        self.frozen = None
        self.ganesha.stop()
        self.ganesha.start()

    def _wait_for_ganesha(self):
        self._wait(lambda: self.ganesha.is_active() and self.ganesha.is_listening(self.port),
                   self.timeout, f'{self.ganesha.name} to come back')

    def _nfs_io(self, name, timeout, wait=True):
        """write a file on the NFS mount and read it back"""
        path = os.path.join(self.nfs_dir, name)
        return self.nfs_remote.run(
            args=['timeout', '-s', 'KILL', str(timeout), 'sh', '-c',
                  f'echo -n {name} > {path} && test "$(cat {path})" = {name}'],
            check_status=False, wait=wait)

    def _check_io(self, name):
        start = time.time()
        proc = self._nfs_io(name, self.io_timeout)
        if proc.exitstatus != 0:
            raise ReclaimFailure(f'{name}: NFS I/O failed or timed out ({proc.exitstatus}) '
                                 f'after {time.time() - start:.0f}s')

    def _hold_rank(self, name):
        """freeze ganesha and fail rank 0: the new MDS has to keep ganesha's session"""
        self._check_io(f'{name}.before')
        sessions = self._ganesha_sessions()
        if not sessions:
            raise RuntimeError(f'{name}: no ganesha session on rank 0')
        for id_, meta in sessions.items():
            if 'timeout' not in meta or 'uuid' not in meta:
                raise RuntimeError(f'{name}: ganesha session {id_} has no timeout '
                                   f'or uuid metadata: {meta}')
        self._wait_for_standby()
        old_gid, _ = self._rank0()
        self._freeze_ganesha()
        log.info(f'{name}: froze ganesha (sessions {list(sessions)}), failing rank 0 ({old_gid})')
        self.failed_at = time.time()
        self._ceph(['mds', 'fail', f'{self.fs_name}:0'])

        def held():
            gid, state = self._rank0()
            if gid in (None, old_gid):
                return False
            if state == 'up:active':
                raise RuntimeError(f'{name}: rank 0 went active, ganesha session(s) '
                                   f'{list(sessions)} were not kept')
            return state == 'up:clientreplay'
        self._wait(held, self.timeout, 'rank 0 to reach up:clientreplay')
        time.sleep(self.hold_check)
        _, state = self._rank0()
        kept = set(self._ganesha_sessions()) & set(sessions)
        if state != 'up:clientreplay' or kept != set(sessions):
            raise RuntimeError(f'{name}: rank 0 is {state} with ganesha session(s) {kept} '
                               f'of {list(sessions)} after {self.hold_check}s')
        log.info(f'{name}: rank 0 held in up:clientreplay by ganesha session(s) {list(sessions)}')
        return sessions

    def _test_restart(self, name):
        """a restarted ganesha reclaims its session and releases rank 0"""
        sessions = self._hold_rank(name)
        uuids = {meta['uuid'] for meta in sessions.values()}
        start = time.time()
        self._restart_ganesha()
        elapsed = self._wait_for_active(start, self.rank_timeout)
        if elapsed is None:
            raise ReclaimFailure(f'{name}: rank 0 still {self._rank0()[1]} {self.rank_timeout}s '
                                 f'after ganesha was restarted, session(s) {list(sessions)} '
                                 f'not reclaimed')
        log.info(f'{name}: rank 0 went active {elapsed:.0f}s after ganesha was restarted')
        self._wait_for_ganesha()
        left = set(self._ganesha_sessions()) & set(sessions)
        if left:
            raise ReclaimFailure(f'{name}: old ganesha session(s) {left} still there')

        def reopened():
            now = self._ganesha_sessions().values()
            return uuids <= {meta.get('uuid') for meta in now}
        self._wait(reopened, self.timeout, f'ganesha sessions with uuid(s) {uuids}')
        self._check_io(f'{name}.after')

    def _test_evict(self, name):
        """evicting ganesha's kept session releases rank 0"""
        sessions = self._hold_rank(name)
        start = time.time()
        for id_ in sessions:
            self._evict(id_)
        elapsed = self._wait_for_active(start, self.rank_timeout)
        if elapsed is None:
            raise ReclaimFailure(f'{name}: rank 0 still {self._rank0()[1]} {self.rank_timeout}s '
                                 f'after evicting ganesha session(s) {list(sessions)}')
        log.info(f'{name}: rank 0 went active {elapsed:.0f}s after the eviction')
        # ganesha's client is blocklisted now
        self._restart_ganesha()
        self._wait_for_ganesha()
        self._check_io(f'{name}.after')

    def _test_late(self, name):
        """a ganesha that missed the reconnect but is still running releases rank 0"""
        sessions = self._hold_rank(name)
        start = time.time()
        self._thaw_ganesha()
        io = self._nfs_io(f'{name}.after', self.observe_timeout, wait=False)
        elapsed = self._wait_for_active(start, self.rank_timeout)
        if elapsed is None:
            self.pending_io.append(io)
            log.error(f'{name}: rank 0 not active {self.rank_timeout}s after ganesha resumed, '
                      f'watching it for up to {self.observe_timeout}s')
            elapsed = self._wait_for_active(start, self.observe_timeout)
            if elapsed is None:
                outcome = f'was still held after {self.observe_timeout}s'
            else:
                # compare with session_autoclose, which counts from the reconnect
                outcome = (f'went active after {elapsed:.0f}s '
                           f'({start + elapsed - self.failed_at:.0f}s after the failover)')
            evicted = set(sessions) - set(self._ganesha_sessions())
            if evicted:
                outcome += f', ganesha session(s) {evicted} evicted'
            if io.finished:
                outcome += f', NFS I/O exited {io.exitstatus}'
            raise ReclaimFailure(f'{name}: rank 0 not active {self.rank_timeout}s after '
                                 f'ganesha resumed: it {outcome} (session_autoclose '
                                 f'{self._fs()["session_autoclose"]}s)')
        log.info(f'{name}: rank 0 went active {elapsed:.0f}s after ganesha resumed')
        io.wait()
        if io.exitstatus != 0:
            raise ReclaimFailure(f'{name}: NFS I/O failed or timed out ({io.exitstatus}) '
                                 f'after ganesha resumed')

    def _recover(self, name):
        """leave rank 0 active and a fresh ganesha for the teardown"""
        _, state = self._rank0()
        if state != 'up:active':
            for id_ in self._ganesha_sessions():
                self._evict(id_)
            if self._wait_for_active(time.time(), self.timeout) is None:
                raise RuntimeError(f'{name}: rank 0 not active after evicting ganesha')
        self._restart_ganesha()
        self._wait_for_ganesha()
        for proc in self.pending_io:
            proc.wait()
        self.pending_io = []
        self._check_io(f'{name}.recovered')

    def begin(self):
        super(GaneshaReclaim, self).begin()

        scenarios = {
            'restart': self._test_restart,
            'evict': self._test_evict,
            'late': self._test_late,
        }
        name = self.config.get('scenario')
        if name not in scenarios:
            raise RuntimeError(f'scenario must be one of {list(scenarios)}, not {name!r}')

        self.cluster_id = self.config['cluster_id']
        self.fs_name = self.config.get('fs_name', 'cephfs')
        self.rank_timeout = self.config.get('rank_timeout', 120)
        self.observe_timeout = self.config.get('observe_timeout', 600)
        self.io_timeout = self.config.get('io_timeout', 300)
        self.hold_check = self.config.get('hold_check', 15)
        self.timeout = self.config.get('timeout', 300)
        nfs_client = self.config.get('nfs_client', 'client.0')

        first_mon = misc.get_first_mon(self.ctx, None)
        (self.mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()
        (self.nfs_remote,) = self.ctx.cluster.only(nfs_client).remotes.keys()
        test_dir = misc.get_testdir(self.ctx)
        # mount point of ganesha-client
        nfs_mnt = os.path.join(test_dir, f'mnt.{nfs_client.split(".")[1]}')
        self.nfs_dir = os.path.join(nfs_mnt, 'reclaim')
        self.nfs_remote.run(args=['mkdir', '-p', self.nfs_dir])

        fsid = self.ctx.ceph['ceph'].fsid
        daemons = _nfs_daemons(self.ctx, self.mon0_remote, self.cluster_id)
        if len(daemons) != 1:
            raise RuntimeError(f'expected one ganesha daemon in nfs cluster {self.cluster_id}, '
                               f'found {[d["daemon_name"] for d in daemons]}')
        self.ganesha = GaneshaDaemon(fsid, daemons[0], _host_remote(self.ctx, daemons[0]['hostname']))
        self.port = _nfs_port(self.mon0_remote, self.cluster_id)

        try:
            scenarios[name](name)
        except ReclaimFailure as e:
            log.error(e)
            try:
                self._recover(name)
            except Exception:
                log.exception(f'{name}: could not recover rank 0 and ganesha')
            raise RuntimeError(str(e)) from e

    def end(self):
        super(GaneshaReclaim, self).end()
        if self.frozen:
            self._thaw_ganesha()

task = GaneshaReclaim
