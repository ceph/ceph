
import logging
import json

from teuthology.task import Task
from teuthology import misc
import ceph_manager

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
    - workunit: ...
    """

    def start(self):
        log.info("START")

    def end(self):
        cluster_name = self.config.get('cluster_name', None)
        dry_run = self.config.get('dry_run', False)
        targets = self.config.get('counters', {})

        if cluster_name is None:
            cluster_name = self.ctx.managers.keys()[0]

        for daemon_type, counters in targets.items():
            # List of 'a', 'b', 'c'...
            daemon_ids = list(misc.all_roles_of_type(self.ctx.cluster, daemon_type))
            daemons = dict([(daemon_id,
                             self.ctx.daemons.get_daemon(daemon_type, daemon_id))
                            for daemon_id in daemon_ids])

            seen = set()

            for daemon_id, daemon in daemons.items():
                if not daemon.running():
                    log.info("Ignoring daemon {0}, it isn't running".format(daemon_id))
                    continue
                else:
                    log.debug("Getting stats from {0}".format(daemon_id))

                manager = self.ctx.managers[cluster_name]
                proc = manager.admin_socket(daemon_type, daemon_id, ["perf", "dump"])
                response_data = proc.stdout.getvalue().strip()
                if response_data:
                    perf_dump = json.loads(response_data)
                else:
                    log.warning("No admin socket response from {0}, skipping".format(daemon_id))
                    continue

                for counter in counters:
                    subsys, counter_id = counter.split(".")
                    if subsys not in perf_dump or counter_id not in perf_dump[subsys]:
                        log.warning("Counter '{0}' not found on daemon {1}.{2}".format(
                            counter, daemon_type, daemon_id))
                        continue
                    value = perf_dump[subsys][counter_id]

                    log.info("Daemon {0}.{1} {2}={3}".format(
                        daemon_type, daemon_id, counter, value
                    ))

                    if value > 0:
                        seen.add(counter)

            if not dry_run:
                unseen = set(counters) - set(seen)
                if unseen:
                    raise RuntimeError("The following counters failed to be set "
                                       "on {0} daemons: {1}".format(
                        daemon_type, unseen
                    ))

task = CheckCounter
