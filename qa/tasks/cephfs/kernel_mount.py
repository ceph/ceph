import json
import logging

from io import StringIO
from textwrap import dedent

from teuthology.orchestra.run import CommandFailedError
from teuthology.orchestra import run
from teuthology.contextutil import MaxWhileTries

from tasks.cephfs.mount import CephFSMount

log = logging.getLogger(__name__)


UMOUNT_TIMEOUT = 300


class KernelMount(CephFSMount):
    def __init__(self, ctx, test_dir, client_id, client_remote,
                 client_keyring_path=None, hostfs_mntpt=None,
                 cephfs_name=None, cephfs_mntpt=None, brxnet=None, config={}):
        super(KernelMount, self).__init__(ctx=ctx, test_dir=test_dir,
            client_id=client_id, client_remote=client_remote,
            client_keyring_path=client_keyring_path, hostfs_mntpt=hostfs_mntpt,
            cephfs_name=cephfs_name, cephfs_mntpt=cephfs_mntpt, brxnet=brxnet)

        self.rbytes = config.get('rbytes', False)

    def mount(self, mntopts=[], createfs=True, check_status=True, **kwargs):
        self.update_attrs(**kwargs)
        self.assert_and_log_minimum_mount_details()

        self.setup_netns()

        # TODO: don't call setupfs() from within mount(), since it's
        # absurd. The proper order should be: create FS first and then
        # call mount().
        if createfs:
            self.setupfs(name=self.cephfs_name)
        if not self.cephfs_mntpt:
            self.cephfs_mntpt = '/'

        stderr = StringIO()
        try:
            self.client_remote.run(args=['mkdir', '-p', self.hostfs_mntpt],
                                   timeout=(5*60), stderr=stderr)
        except CommandFailedError:
            if 'file exists' not in stderr.getvalue().lower():
                raise

        retval = self._run_mount_cmd(mntopts, check_status)
        if retval:
            return retval

        stderr = StringIO()
        try:
            self.client_remote.run(
                args=['sudo', 'chmod', '1777', self.hostfs_mntpt],
                stderr=stderr, timeout=(5*60))
        except CommandFailedError:
            # the client does not have write permissions in the caps it holds
            # for the Ceph FS that was just mounted.
            if 'permission denied' in stderr.getvalue().lower():
                pass


        self.mounted = True

    def _run_mount_cmd(self, mntopts, check_status):
        opts = 'norequire_active_mds'
        if self.client_id:
            opts += ',name=' + self.client_id
        if self.client_keyring_path and self.client_id:
            opts += ',secret=' + self.get_key_from_keyfile()
        if self.config_path:
            opts += ',conf=' + self.config_path
        if self.cephfs_name:
            opts += ",mds_namespace=" + self.cephfs_name
        if self.rbytes:
            opts += ",rbytes"
        else:
            opts += ",norbytes"
        if mntopts:
            opts += ',' + ','.join(mntopts)

        mount_dev = ':' + self.cephfs_mntpt
        prefix = ['sudo', 'adjust-ulimits', 'ceph-coverage',
                  self.test_dir + '/archive/coverage',
                  'nsenter',
                  '--net=/var/run/netns/{0}'.format(self.netns_name)]
        cmdargs = prefix + ['/bin/mount', '-t', 'ceph', mount_dev,
                            self.hostfs_mntpt, '-v', '-o', opts]

        mountcmd_stdout, mountcmd_stderr = StringIO(), StringIO()
        try:
            self.client_remote.run(args=cmdargs, timeout=(30*60),
                                   stdout=mountcmd_stdout,
                                   stderr=mountcmd_stderr)
        except CommandFailedError as e:
            log.info('mount command failed')
            if check_status:
                raise
            else:
                return (e, mountcmd_stdout.getvalue(),
                        mountcmd_stderr.getvalue())
        log.info('mount command passed')

    def umount(self, force=False):
        if not self.is_mounted():
            self.cleanup()
            return

        log.debug('Unmounting client client.{id}...'.format(id=self.client_id))

        try:
            cmd=['sudo', 'umount', self.hostfs_mntpt]
            if force:
                cmd.append('-f')
            self.client_remote.run(args=cmd, timeout=(15*60), omit_sudo=False)
        except Exception as e:
            self.client_remote.run(
                args=['sudo', run.Raw('PATH=/usr/sbin:$PATH'), 'lsof',
                      run.Raw(';'), 'ps', 'auxf'],
                timeout=(15*60), omit_sudo=False)
            raise e

        self.mounted = False
        self.cleanup()

    def umount_wait(self, force=False, require_clean=False, timeout=900):
        """
        Unlike the fuse client, the kernel client's umount is immediate
        """
        if not self.is_mounted():
            self.cleanup()
            return

        try:
            self.umount(force)
        except (CommandFailedError, MaxWhileTries):
            if not force:
                raise

            # force delete the netns and umount
            self.client_remote.run(args=['sudo', 'umount', '-f', '-l',
                                         self.mountpoint],
                                   timeout=(15*60), omit_sudo=False)

            self.mounted = False
            self.cleanup()

    def wait_until_mounted(self):
        """
        Unlike the fuse client, the kernel client is up and running as soon
        as the initial mount() function returns.
        """
        assert self.mounted

    def teardown(self):
        super(KernelMount, self).teardown()
        if self.mounted:
            self.umount()

    def _find_debug_dir(self):
        """
        Find the debugfs folder for this mount
        """
        pyscript = dedent("""
            import glob
            import os
            import json

            def get_id_to_dir():
                result = {}
                for dir in glob.glob("/sys/kernel/debug/ceph/*"):
                    mds_sessions_lines = open(os.path.join(dir, "mds_sessions")).readlines()
                    client_id = mds_sessions_lines[1].split()[1].strip('"')

                    result[client_id] = dir
                return result

            print(json.dumps(get_id_to_dir()))
            """)

        output = self.client_remote.sh([
            'sudo', 'python3', '-c', pyscript
        ], timeout=(5*60))
        client_id_to_dir = json.loads(output)

        try:
            return client_id_to_dir[self.client_id]
        except KeyError:
            log.error("Client id '{0}' debug dir not found (clients seen were: {1})".format(
                self.client_id, ",".join(client_id_to_dir.keys())
            ))
            raise

    def _read_debug_file(self, filename):
        debug_dir = self._find_debug_dir()

        pyscript = dedent("""
            import os

            print(open(os.path.join("{debug_dir}", "{filename}")).read())
            """).format(debug_dir=debug_dir, filename=filename)

        output = self.client_remote.sh([
            'sudo', 'python3', '-c', pyscript
        ], timeout=(5*60))
        return output

    def get_global_id(self):
        """
        Look up the CephFS client ID for this mount, using debugfs.
        """

        assert self.mounted

        mds_sessions = self._read_debug_file("mds_sessions")
        lines = mds_sessions.split("\n")
        return int(lines[0].split()[1])

    def get_osd_epoch(self):
        """
        Return 2-tuple of osd_epoch, osd_epoch_barrier
        """
        osd_map = self._read_debug_file("osdmap")
        lines = osd_map.split("\n")
        first_line_tokens = lines[0].split()
        epoch, barrier = int(first_line_tokens[1]), int(first_line_tokens[3])

        return epoch, barrier
