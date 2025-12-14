import logging
import signal
import time
import traceback

from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.event import Event

log = logging.getLogger(__name__)

class BarkError(Exception):
    """
    An exception for the watchdog bark. We can use this as an exception
    higer up in the stack to give a reason why the test failed
    """

    def __init__(self, bark_reason):
        self.message = f"Watchdog timeout: {bark_reason}"
        super().__init__(self.message)

class DaemonWatchdog(Greenlet):
    """
    DaemonWatchdog::

    This process monitors 3 classes of running processes for failures:
    1. Ceph daemons e.g OSDs
    2. Thrashers - These are test processes used to create errors during
                   testing e.g. RBDMirrorThrasher in qa/tasks/rbd_mirror_thrash.py
    3. Processes - Any WatchedProcess that is running as part of a test suite.
                   These processses are typically things like I/O exercisers
                   e.g. CephTestRados in qa/tasks/rados.py

    If an extended failure is detected in a daemon process (i.e. not intentional), then
    the watchdog will bark. It will also bark is an assert is raised during the running
    of any monitored Thrashers or WatchedProcesses
    
    When the watchdog barks it will 
     - unmount file systems and send SIGTERM to all daemons.
     - stop all thrasher processes by calling their stop_and_join() method
     - stop any watched processes by calling their stop() method

    The duration of an extended failure is configurable with watchdog_daemon_timeout.

    ceph:
      watchdog:
        daemon_restart [default: no]: restart daemon if "normal" exit (status==0).

        daemon_timeout [default: 300]: number of seconds a daemon
                                              is allowed to be failed before the
                                              watchdog will bark.
    """

    def __init__(self, ctx, config):
        super(DaemonWatchdog, self).__init__()
        self.config = ctx.config.get('watchdog', {})
        self.ctx = ctx
        self.e = None
        self.logger = log.getChild('daemon_watchdog')
        self.cluster = config.get('cluster', 'ceph')
        self.name = 'watchdog'
        self.stopping = Event()
        self.thrashers = ctx.ceph[config["cluster"]].thrashers
        self.watched_processes = ctx.ceph[config["cluster"]].watched_processes
        self.assert_trackers = ctx.ceph[config["cluster"]].assert_trackers

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
        self.stopping.set()

    def bark(self, reason):
        self.log("BARK! unmounting mounts and killing all daemons")
        if hasattr(self.ctx, 'mounts'):
            for mount in self.ctx.mounts.values():
                try:
                    mount.umount_wait(force=True)
                except:
                    self.logger.exception("ignoring exception:")
        daemons = []
        daemons.extend(filter(lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role('osd', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role('mds', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role('mon', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role('rgw', cluster=self.cluster)))
        daemons.extend(filter(lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role('mgr', cluster=self.cluster)))

        for daemon in daemons:
            try:
                daemon.signal(signal.SIGTERM)
            except:
                self.logger.exception("ignoring exception:")

        for thrasher in self.thrashers:
            self.log("Killing thrasher {name}".format(name=thrasher.name))
            thrasher.stop_and_join()

        for proc in self.watched_processes:
            self.log("Killing remote process {process_id}".format(process_id=proc.id))
            proc.set_exception(BarkError(reason))
            proc.stop()

    def watch(self):
        self.log("watchdog starting")
        daemon_timeout = int(self.config.get('daemon_timeout', 300))
        daemon_restart = self.config.get('daemon_restart', False)
        daemon_failure_time = {}
        bark_reason = []
        while not self.stopping.is_set():
            bark = False
            now = time.time()

            osds = self.ctx.daemons.iter_daemons_of_role('osd', cluster=self.cluster)
            mons = self.ctx.daemons.iter_daemons_of_role('mon', cluster=self.cluster)
            mdss = self.ctx.daemons.iter_daemons_of_role('mds', cluster=self.cluster)
            rgws = self.ctx.daemons.iter_daemons_of_role('rgw', cluster=self.cluster)
            mgrs = self.ctx.daemons.iter_daemons_of_role('mgr', cluster=self.cluster)

            daemon_failures = []
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), osds))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), mons))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), mdss))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), rgws))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), mgrs))

            def shorten_failure_list(failures, reason):
                if len(failures) > 1:
                    # Remove common prefixes from failure list
                    last=""
                    failures2 = []
                    for f in failures:
                        p = last.split(".")
                        n = f.split(".")
                        if p[0] != n[0] or len(p) == 1 or len(n) == 1:
                            failures2.append(f)
                        elif p[1] != n[1] or len(n) == 2:
                            failures2.append(".".join(n[1:]))
                        else:
                            failures2.append(".".join(n[2:]))
                        last = f
                    failure_list = ",".join(failures2)
                    bark_reason.append(f"Multiple daemons {failure_list} have {reason}")
                elif len(failures) == 1:
                    bark_reason.append(f"Daemon {failures[0]} has {reason}")

            try:
                earliest = None
                failures = []
                for daemon in daemon_failures:
                    name = daemon.role + '.' + daemon.id_
                    dt = daemon_failure_time.setdefault(name, (daemon, now))
                    assert dt[0] is daemon
                    delta = now-dt[1]
                    self.log("daemon {name} is failed for ~{t:.0f}s".format(name=name, t=delta))
                    if delta > daemon_timeout:
                        bark = True
                        failures.append(f"{name}")
                        for at in self.assert_trackers:
                            if at.match_id(daemon.role, daemon.id_):
                                exception = at.get_exception()
                                if exception is not None:
                                    if earliest == None or at.get_failure_time() < earliest.get_failure_time():
                                        earliest = at

                    if daemon_restart == 'normal' and daemon.proc.exitstatus == 0:
                        self.log(f"attempting to restart daemon {name}")
                        daemon.restart()

                shorten_failure_list(failures, "timed out")

                # If a daemon is no longer failed, remove it from tracking:
                for name in list(daemon_failure_time.keys()):
                    if name not in [d.role + '.' + d.id_ for d in daemon_failures]:
                        self.log("daemon {name} has been restored".format(name=name))
                        del daemon_failure_time[name]

                for thrasher in self.thrashers:
                    if thrasher.exception is not None:
                        self.log("{name} failed".format(name=thrasher.name))
                        bark_reason.append(f"Thrasher {name} threw exception {thrasher.exception}")
                        bark = True

                for proc in self.watched_processes:
                    if proc.exception is not None:
                        self.log("Remote process %s failed" % proc.id)
                        bark_reason.append(f"Remote process {proc.id} threw exception {proc.exception}")
                        bark = True

                # If a thrasher or watched process failed then check for the earliest daemon failure
                failures = []
                if bark and earliest == None:
                    for at in self.assert_trackers:
                        exception = at.get_exception()
                        if exception is not None:
                            failures.append(f"{at.role}.{at.id_}")
                            if earliest == None or at.get_failure_time() < earliest.get_failure_time():
                                earliest = at

                shorten_failure_list(failures, "crashed")

                # Only report details about one daemon failure
                if earliest != None:
                    bark_reason.append(f"First crash: Daemon {earliest.role}.{earliest.id_} at {time.ctime(earliest.get_failure_time())}")
                    bark_reason.append(f"{earliest.get_exception()}")

            except:
                bark = True
                bark_reason.append(f"Watchdog bug {traceback.format_exc()}")

            if bark:
                self.bark("\n".join(bark_reason))
                return

            sleep(5)

        self.log("watchdog finished")
