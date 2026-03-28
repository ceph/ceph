
import logging
import json
import errno

from teuthology.task import Task
from teuthology import misc

from tasks import ceph_manager
from tasks.cephfs.filesystem import MDSCluster
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class CheckCounter(Task):
    """
    Use this task to validate that some daemon perf counters were
    incremented by the nested tasks.

    Config:
     'cluster_name': optional, specify which cluster
     'target': dictionary of daemon type to list of performance counters.
     'dry_run': just log the value of the counters, don't fail if they
                aren't nonzero.

    Success condition is that for all of the named counters, at least
    one of the daemons of that type has the counter nonzero.

    Example to check cephfs dirfrag splits are happening:
    - install:
    - ceph:
    - ceph-fuse:
    - check-counter:
        counters:
            mds:
                - "mds.dir_split"
                -
                    name: "mds.dir_update"
                    min: 3
    - workunit: ...
    """
    @property
    def admin_remote(self):
        first_mon = misc.get_first_mon(self.ctx, None)
        (result,) = self.ctx.cluster.only(first_mon).remotes.keys()
        return result

    def start(self):
        log.info("START")

    def end(self):
        overrides = self.ctx.config.get('overrides', {})
        misc.deep_merge(self.config, overrides.get('check-counter', {}))

        cluster_name = self.config.get('cluster_name', None)
        dry_run = self.config.get('dry_run', False)
        targets = self.config.get('counters', {})

        if cluster_name is None:
            cluster_name = next(iter(self.ctx.managers.keys()))


        mon_manager = ceph_manager.CephManager(self.admin_remote, ctx=self.ctx, logger=log.getChild('ceph_manager'))
        active_mgr = json.loads(mon_manager.raw_cluster_cmd("mgr", "dump", "--format=json-pretty"))["active_name"]

        mds_cluster = MDSCluster(self.ctx)
        status = mds_cluster.status()

        for daemon_type, counters in targets.items():
            # List of 'a', 'b', 'c'...
            daemon_ids = list(misc.all_roles_of_type(self.ctx.cluster, daemon_type))
            daemons = dict([(daemon_id,
                             self.ctx.daemons.get_daemon(daemon_type, daemon_id))
                            for daemon_id in daemon_ids])

            expected = set()
            seen = set()

            for daemon_id, daemon in daemons.items():
                if not daemon.running():
                    log.info(f"Ignoring daemon {daemon_type}.{daemon_id}, it isn't running")
                    continue
                elif daemon_type == 'mgr' and daemon_id != active_mgr:
                    continue

                if daemon_type == 'mds':
                    mds_info = status.get_mds(daemon_id)
                    if not mds_info:
                        continue
                    mds = f"mds.{mds_info['gid']}"
                    if mds_info['state'] != "up:active":
                        log.debug(f"skipping {mds}")
                        continue
                    log.debug(f"Getting stats from {mds}")
                    try:
                        proc = mon_manager.raw_cluster_cmd("tell", mds, "perf", "dump",
                                                           "--format=json-pretty")
                        response_data = proc.strip()
                    except CommandFailedError as e:
                        if e.exitstatus == errno.ENOENT:
                            log.debug(f"Failed to do 'perf dump' on {mds}")
                        continue
                else:
                    log.debug(f"Getting stats from {daemon_type}.{daemon_id}")
                    manager = self.ctx.managers[cluster_name]
                    proc = manager.admin_socket(daemon_type, daemon_id, ["perf", "dump"])
                    response_data = proc.stdout.getvalue().strip()
                if response_data:
                    perf_dump = json.loads(response_data)
                else:
                    log.warning(f"No response from {daemon_type}.{daemon_id}, skipping")
                    continue

                minval = ''
                expected_val = ''
                for counter in counters:
                    if isinstance(counter, dict):
                        name = counter['name']
                        if 'min' in counter:
                            minval = counter['min']
                        if 'expected_val' in counter:
                            expected_val = counter['expected_val']
                    else:
                        name = counter
                        minval = 1
                    expected.add(name)

                    val = perf_dump
                    for key in name.split('.'):
                        if key not in val:
                            log.warning(f"Counter '{name}' not found on daemon {daemon_type}.{daemon_id}")
                            val = None
                            break

                        val = val[key]

                    if val is not None:
                        if (isinstance(minval, int) and val >= minval) or \
                           (isinstance(expected_val, int) and val == expected_val):
                            log.info(f"Found daemon: {daemon_type}.{daemon_id}, "
                                     f"counter: {name}={val} in perf dump")
                            seen.add(name)

            if not dry_run:
                unseen = set(expected) - set(seen)
                if unseen:
                    raise RuntimeError("The following counters failed to be set "
                                       f"on {daemon_type} daemon: {unseen}")
                log.info(f"Expected counters {expected} passed to be set on {daemon_type} daemon")

task = CheckCounter
