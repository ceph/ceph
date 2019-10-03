import logging
import os

from teuthology.orchestra.cluster import Cluster
from teuthology.exit import exiter
from teuthology.task import Task

log = logging.getLogger(__name__)


class ConsoleLog(Task):
    enabled = True
    name = 'console_log'
    logfile_name = '{shortname}.log'

    def __init__(self, ctx=None, config=None):
        super(ConsoleLog, self).__init__(ctx, config)
        if self.config.get('enabled') is False:
            self.enabled = False
        if not getattr(self.ctx, 'archive', None):
            self.enabled = False
        if 'logfile_name' in self.config:
            self.logfile_name = self.config['logfile_name']
        if 'remotes' in self.config:
            self.remotes = self.config['remotes']

    def filter_hosts(self):
        super(ConsoleLog, self).filter_hosts()
        if not hasattr(self.ctx, 'cluster'):
            return
        new_cluster = Cluster()
        for (remote, roles) in self.cluster.remotes.items():
            if not hasattr(remote.console, 'spawn_sol_log'):
                log.debug("%s does not support IPMI; excluding",
                          remote.shortname)
            elif not (remote.console.has_ipmi_credentials or
                      remote.console.has_conserver):
                log.debug("Cannot find IPMI credentials or conserver settings "
                          "for %s; excluding",
                          remote.shortname)
            else:
                new_cluster.add(remote, roles)
        self.cluster = new_cluster
        self.remotes = self.cluster.remotes.keys()
        return self.cluster

    def setup(self):
        if not self.enabled:
            return
        super(ConsoleLog, self).setup()
        self.processes = dict()
        self.signal_handlers = list()
        self.setup_archive()

    def setup_archive(self):
        self.archive_dir = os.path.join(
            self.ctx.archive,
            'console_logs',
        )
        if not os.path.isdir(self.archive_dir):
            os.makedirs(self.archive_dir)

    def begin(self):
        if not self.enabled:
            return
        super(ConsoleLog, self).begin()
        self.start_logging()

    def start_logging(self):
        for remote in self.remotes:
            log_path = os.path.join(
                self.archive_dir,
                self.logfile_name.format(shortname=remote.shortname),
            )
            proc = remote.console.spawn_sol_log(log_path)
            self.processes[remote.shortname] = proc

        # Install a signal handler to make sure the console-logging
        # processes are terminated if the job is killed
        def kill_console_loggers(signal_, frame):
            for (name, proc) in self.processes.items():
                log.debug("Killing console logger for %s", name)
                proc.terminate()
        exiter.add_handler(15, kill_console_loggers)

    def end(self):
        if not self.enabled:
            return
        super(ConsoleLog, self).end()
        self.stop_logging()

    def stop_logging(self, force=False):
        for proc in self.processes.values():
            if proc.poll() is not None:
                continue
            if force:
                proc.kill()
            else:
                proc.terminate()

        # Remove any signal handlers
        for handler in self.signal_handlers:
            handler.remove()

    def teardown(self):
        if not self.enabled:
            return
        self.stop_logging(force=True)
        super(ConsoleLog, self).teardown()


task = ConsoleLog
