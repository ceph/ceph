import copy
from io import StringIO
import json
import logging
import signal

from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.event import Event

log = logging.getLogger(__name__)

class WatchMempools(Greenlet):
    """
    WatchMempools::

    Watch mempools during teuthology run and collect memory usege during intervals.

    ceph:
      watchmempools:
        interval [default: 60]: interval in seconds to collect memory usage.
        memory_max_diff [default: 10]: trigger warning message in case one of the mempools
                                        memory usage changed by more than this percentage.
        total_memory_max_diff [default: 30]: trigger warning message in case total memory
                                                usage changed by more than this percentage.
    """

    def __init__(self, ctx, config):
        super(WatchMempools, self).__init__()
        self.config = ctx.config.get('watchmempools', {})
        self.ctx = ctx
        self.cluster = config.get('cluster', 'ceph')
        self.ceph_manager = ctx.managers[self.cluster]
        self.e = None
        self.logger = log.getChild('mempools_watch')
        self.cluster = config.get('cluster', 'ceph')
        self.name = 'watchmempools'
        self.stopping = Event()

    def _run(self):
        try:
            self.watch()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.e = e
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def log(self, x):
        """Write data to logger"""
        self.logger.info(x)

    def stop(self):
        self.log("stopping watchmempools")
        self.stopping.set()

    def _kill(self):
        self.log("unmounting mounts and killing all daemons")
        if hasattr(self.ctx, 'mounts'):
            for mount in self.ctx.mounts.values():
                try:
                    mount.umount_wait(force=True)
                except:
                    self.logger.exception("ignoring exception:")
        daemons = []
        daemons.extend(filter(lambda daemon: daemon.running() and not daemon.proc.finished, self.ctx.daemons.iter_daemons_of_role('osd', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: daemon.running() and not daemon.proc.finished, self.ctx.daemons.iter_daemons_of_role('mds', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: daemon.running() and not daemon.proc.finished, self.ctx.daemons.iter_daemons_of_role('mon', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: daemon.running() and not daemon.proc.finished, self.ctx.daemons.iter_daemons_of_role('rgw', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: daemon.running() and not daemon.proc.finished, self.ctx.daemons.iter_daemons_of_role('mgr', cluster=self.cluster)))

        for daemon in daemons:
            try:
                daemon.signal(signal.SIGTERM)
            except:
                self.logger.exception("ignoring exception:")

    def bytes_per_item(self, pool_data):
        if pool_data["items"] != 0:
            return pool_data["bytes"] / pool_data["items"]
        else:
            return float("inf")

    def watch(self):
        self.log("watchmempools starting")
        interval = int(self.config.get('interval', 60))
        osd_status = self.ceph_manager.get_osd_status()
        self.up_osds = osd_status['up']
        prev_dump = {}
        kill_it = False

        while len(self.up_osds):
            for osd_id in self.up_osds:
                self.log(f"watchmempools: collecting memory usage for osd.{osd_id}")
                remote = self.ceph_manager.find_remote('osd', osd_id)
                proc = remote.run(args=['sudo', 'ceph', 'tell', f'osd.{osd_id}', 'dump_mempools'],
                                stdout=StringIO(),
                                wait=True)

                mempool_data = json.loads(proc.stdout.getvalue())
                if prev_dump.get(osd_id) is None:
                    prev_dump[osd_id] = copy.deepcopy(mempool_data)
                    continue

                by_pool = mempool_data["mempool"]["by_pool"]
                # ignore cache pools
                by_pool = {k: v for k, v in by_pool.items() if "cache" not in k}
                for pool_name, pool_data in by_pool.items():
                    new_bpi = self.bytes_per_item(pool_data)
                    old_bpi = self.bytes_per_item(prev_dump[osd_id]["mempool"]["by_pool"][pool_name])
                    diff_percent = abs(new_bpi - old_bpi) / old_bpi * 100

                    if diff_percent > int(self.config.get('memory_max_diff', 10)):
                        self.log(f"watchmempools warning: osd.{osd_id} pool {pool_name} memory usage "
                                 f"changed by {diff_percent}% since last interval")

                total_bytes = mempool_data["mempool"]["total"]["bytes"]
                target_total_bytes = int(self.ceph_manager.get_config('osd', osd_id, 'osd_memory_target'))
                total_diff_percent = (total_bytes / target_total_bytes) * 100

                self.log(f"watchmempools: osd.{osd_id} memory usage: {total_bytes} target {target_total_bytes}")
                if total_diff_percent > int(self.config.get('total_memory_max_diff', 30)):
                    self.log(f"watchmempools error: osd.{osd_id} memory usage changed by {total_diff_percent}% "
                             f"since last interval")
                    kill_it = True

                prev_dump[osd_id] = copy.deepcopy(mempool_data)

            if kill_it:
                self._kill()
                return

            sleep(interval)
            osd_status = self.ceph_manager.get_osd_status()
            self.up_osds = osd_status['up']

        self.log("watchmempools finished")

