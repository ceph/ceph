import errno
import json
import logging
import os
import re

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

        self.client_config = config
        self.rbytes = config.get('rbytes', False)
        self.snapdirname = config.get('snapdirname', '.snap')
        self.inst = None
        self.addr = None

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
        if self.snapdirname != '.snap':
            opts += f',snapdirname={self.snapdirname}'
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

    def _get_debug_dir(self):
        """
        Get the debugfs folder for this mount
        """

        cluster_name = 'ceph'
        fsid = self.ctx.ceph[cluster_name].fsid

        global_id = self._get_global_id()

        return os.path.join("/sys/kernel/debug/ceph/", f"{fsid}.client{global_id}")

    def read_debug_file(self, filename):
        """
        Read the debug file "filename", return None if the file doesn't exist.
        """

        path = os.path.join(self._get_debug_dir(), filename)

        stdout = StringIO()
        stderr = StringIO()
        try:
            self.run_shell_payload(f"sudo dd if={path}", timeout=(5 * 60),
                                   stdout=stdout, stderr=stderr)
            return stdout.getvalue()
        except CommandFailedError:
            if 'no such file or directory' in stderr.getvalue().lower():
                return errno.ENOENT
            elif 'not a directory' in stderr.getvalue().lower():
                return errno.ENOTDIR
            elif 'permission denied' in stderr.getvalue().lower():
                return errno.EACCES
            raise

    def _get_global_id(self):
        try:
            p = self.run_shell_payload("getfattr --only-values -n ceph.client_id .", stdout=StringIO())
            v = p.stdout.getvalue()
            prefix = "client"
            assert v.startswith(prefix)
            return int(v[len(prefix):])
        except CommandFailedError:
            # Probably this fallback can be deleted in a few releases when the kernel xattr is widely available.
            log.debug("Falling back to messy global_id lookup via /sys...")

            pyscript = dedent("""
                import glob
                import os
                import json

                def get_id_to_dir():
                    result = {}
                    for dir in glob.glob("/sys/kernel/debug/ceph/*"):
                        mds_sessions_lines = open(os.path.join(dir, "mds_sessions")).readlines()
                        global_id = mds_sessions_lines[0].split()[1].strip('"')
                        client_id = mds_sessions_lines[1].split()[1].strip('"')
                        result[client_id] = global_id
                    return result
                print(json.dumps(get_id_to_dir()))
            """)

            output = self.client_remote.sh([
                'sudo', 'python3', '-c', pyscript
            ], timeout=(5*60))
            client_id_to_global_id = json.loads(output)

            try:
                return client_id_to_global_id[self.client_id]
            except KeyError:
                log.error("Client id '{0}' debug dir not found (clients seen were: {1})".format(
                    self.client_id, ",".join(client_id_to_global_id.keys())
                ))
                raise

    def get_global_id(self):
        """
        Look up the CephFS client ID for this mount, using debugfs.
        """

        assert self.mounted

        return self._get_global_id()

    @property
    def _global_addr(self):
        if self.addr is not None:
            return self.addr

        # The first line of the "status" file's output will be something
        # like:
        #   "instance: client.4297 (0)10.72.47.117:0/1148470933"
        # What we need here is only the string "10.72.47.117:0/1148470933"
        status = self.read_debug_file("status")
        if status is None:
            return None

        instance = re.findall(r'instance:.*', status)[0]
        self.addr = instance.split()[2].split(')')[1]
        return self.addr;

    @property
    def _global_inst(self):
        if self.inst is not None:
            return self.inst

        client_gid = "client%d" % self.get_global_id()
        self.inst = " ".join([client_gid, self._global_addr])
        return self.inst

    def get_global_inst(self):
        """
        Look up the CephFS client instance for this mount
        """
        return self._global_inst

    def get_global_addr(self):
        """
        Look up the CephFS client addr for this mount
        """
        return self._global_addr

    def get_osd_epoch(self):
        """
        Return 2-tuple of osd_epoch, osd_epoch_barrier
        """
        osd_map = self.read_debug_file("osdmap")
        assert osd_map

        lines = osd_map.split("\n")
        first_line_tokens = lines[0].split()
        epoch, barrier = int(first_line_tokens[1]), int(first_line_tokens[3])

        return epoch, barrier

    def get_op_read_count(self):
        stdout = StringIO()
        stderr = StringIO()
        try:
            path = os.path.join(self._get_debug_dir(), "metrics/size")
            self.run_shell(f"sudo stat {path}", stdout=stdout,
                           stderr=stderr, cwd=None)
            buf = self.read_debug_file("metrics/size")
        except CommandFailedError:
            if 'no such file or directory' in stderr.getvalue().lower() \
                    or 'not a directory' in stderr.getvalue().lower():
                try:
                    path = os.path.join(self._get_debug_dir(), "metrics")
                    self.run_shell(f"sudo stat {path}", stdout=stdout,
                                   stderr=stderr, cwd=None)
                    buf = self.read_debug_file("metrics")
                except CommandFailedError:
                    return errno.ENOENT
            else:
                return 0
        return int(re.findall(r'read.*', buf)[0].split()[1])
