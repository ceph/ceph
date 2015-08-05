import logging

from cStringIO import StringIO

from teuthology.exceptions import SELinuxError
from teuthology.orchestra.cluster import Cluster

from . import Task

log = logging.getLogger(__name__)


class SELinux(Task):
    """
    A task to set the SELinux mode during test execution. Note that SELinux
    must first be enabled and the filesystem must have been labeled.

    On teardown, also checks the audit log for any denials.

    Automatically skips hosts running non-RPM-based OSes.
    """
    def __init__(self, ctx, config):
        super(SELinux, self).__init__(ctx, config)
        self.log = log
        self.mode = self.config.get('mode', 'permissive')

    def filter_hosts(self):
        """
        Exclude any non-RPM-based hosts, and any downburst VMs
        """
        super(SELinux, self).filter_hosts()
        new_cluster = Cluster()
        for (remote, roles) in self.cluster.remotes.iteritems():
            if remote.shortname.startswith('vpm'):
                msg = "Excluding {host}: downburst VMs are not yet supported"
                log.info(msg.format(host=remote.shortname))
            elif remote.os.package_type == 'rpm':
                new_cluster.add(remote, roles)
            else:
                msg = "Excluding {host}: OS '{os}' does not support SELinux"
                log.debug(msg.format(host=remote.shortname, os=remote.os.name))
        self.cluster = new_cluster
        return self.cluster

    def setup(self):
        super(SELinux, self).setup()
        self.rotate_log()
        self.old_modes = self.get_modes()
        self.old_denials = self.get_denials()
        self.set_mode()

    def rotate_log(self):
        self.cluster.run(args="sudo service auditd rotate")

    def get_modes(self):
        """
        Get the current SELinux mode from each host so that we can restore
        during teardown
        """

        log.debug("Getting current SELinux state")
        modes = dict()
        for remote in self.cluster.remotes.iterkeys():
            result = remote.run(
                args=['/usr/sbin/getenforce'],
                stdout=StringIO(),
            )
            modes[remote.name] = result.stdout.getvalue().strip().lower()
        log.debug("Existing SELinux modes: %s", modes)
        return modes

    def set_mode(self):
        """
        Set the requested SELinux mode
        """
        log.info("Putting SELinux into %s mode", self.mode)
        for remote in self.cluster.remotes.iterkeys():
            remote.run(
                args=['sudo', '/usr/sbin/setenforce', self.mode],
            )

    def get_denials(self):
        """
        Look for denials in the audit log
        """
        all_denials = dict()
        for remote in self.cluster.remotes.iterkeys():
            proc = remote.run(
                args=['sudo', 'grep', 'avc: .*denied',
                      '/var/log/audit/audit.log'],
                stdout=StringIO(),
                check_status=False,
            )
            output = proc.stdout.getvalue()
            if output:
                denials = output.strip().split('\n')
                log.debug("%s has %s denials", remote.name, len(denials))
            else:
                denials = []
            all_denials[remote.name] = denials
        return all_denials

    def teardown(self):
        self.restore_modes()
        self.get_new_denials()

    def restore_modes(self):
        """
        If necessary, restore previous SELinux modes
        """
        # If there's nothing to do, skip this
        if not set(self.old_modes.values()).difference(set([self.mode])):
            return
        log.info("Restoring old SELinux modes")
        for remote in self.cluster.remotes.iterkeys():
            mode = self.old_modes[remote.name]
            if mode != self.mode:
                remote.run(
                    args=['sudo', '/usr/sbin/setenforce', mode],
                )

    def get_new_denials(self):
        """
        Determine if there are any new denials in the audit log
        """
        all_denials = self.get_denials()
        new_denials = dict()
        for remote in self.cluster.remotes.iterkeys():
            old_host_denials = self.old_denials[remote.name]
            all_host_denials = all_denials[remote.name]
            new_host_denials = set(all_host_denials).difference(
                set(old_host_denials)
            )
            new_denials[remote.name] = list(new_host_denials)

        for remote in self.cluster.remotes.iterkeys():
            if len(new_denials[remote.name]):
                raise SELinuxError(node=remote,
                                   denials=new_denials[remote.name])

task = SELinux
