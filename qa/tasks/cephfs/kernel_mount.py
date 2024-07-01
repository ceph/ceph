import errno
import json
import logging
import os
import re

from io import StringIO
from textwrap import dedent

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from teuthology.contextutil import MaxWhileTries

from tasks.cephfs.mount import CephFSMount

log = logging.getLogger(__name__)


UMOUNT_TIMEOUT = 300
# internal metadata directory
DEBUGFS_META_DIR = 'meta'

class KernelMount(CephFSMount):
    def __init__(self, ctx, test_dir, client_id, client_remote,
                 client_keyring_path=None, hostfs_mntpt=None,
                 cephfs_name=None, cephfs_mntpt=None, brxnet=None,
                 client_config={}):
        super(KernelMount, self).__init__(ctx=ctx, test_dir=test_dir,
            client_id=client_id, client_remote=client_remote,
            client_keyring_path=client_keyring_path, hostfs_mntpt=hostfs_mntpt,
            cephfs_name=cephfs_name, cephfs_mntpt=cephfs_mntpt, brxnet=brxnet)

        self.client_config = client_config
        self.dynamic_debug = client_config.get('dynamic_debug', False)
        self.rbytes = client_config.get('rbytes', False)
        self.snapdirname = client_config.get('snapdirname', '.snap')
        self.syntax_style = client_config.get('syntax', 'v2')
        self.inst = None
        self.addr = None
        self._mount_bin = ['adjust-ulimits', 'ceph-coverage', self.test_dir +\
                           '/archive/coverage', '/bin/mount', '-t', 'ceph']

    def mount(self, mntopts=[], check_status=True, **kwargs):
        self.update_attrs(**kwargs)
        self.assert_and_log_minimum_mount_details()

        self.setup_netns()

        if not self.cephfs_mntpt:
            self.cephfs_mntpt = '/'
        if not self.cephfs_name:
            self.cephfs_name = 'cephfs'

        self._create_mntpt()

        retval = self._run_mount_cmd(mntopts, check_status)
        if retval:
            return retval

        self._set_filemode_on_mntpt()

        if self.dynamic_debug:
            kmount_count = self.ctx.get(f'kmount_count.{self.client_remote.hostname}', 0)
            if kmount_count == 0:
                self.enable_dynamic_debug()
            self.ctx[f'kmount_count.{self.client_remote.hostname}'] = kmount_count + 1

        self.mounted = True

    def _run_mount_cmd(self, mntopts, check_status):
        mount_cmd = self._get_mount_cmd(mntopts)
        mountcmd_stdout, mountcmd_stderr = StringIO(), StringIO()

        try:
            self.client_remote.run(args=mount_cmd, timeout=(30*60),
                                   stdout=mountcmd_stdout,
                                   stderr=mountcmd_stderr, omit_sudo=False)
        except CommandFailedError as e:
            log.info('mount command failed')
            if check_status:
                raise
            else:
                return (e, mountcmd_stdout.getvalue(),
                        mountcmd_stderr.getvalue())
        log.info('mount command passed')

    def _make_mount_cmd_old_or_new_style(self):
        optd = {}
        mnt_stx = ''
        if self.syntax_style == 'v1':
            mnt_stx = f':{self.cephfs_mntpt}'
            if self.client_id:
                optd['name'] = self.client_id
            if self.cephfs_name:
                optd['mds_namespace'] = self.cephfs_name
        elif self.syntax_style == 'v2':
            mnt_stx = f'{self.client_id}@.{self.cephfs_name}={self.cephfs_mntpt}'
        else:
            assert 0, f'invalid syntax style: {self.syntax_style}'
        return (mnt_stx, optd)

    def _get_mount_cmd(self, mntopts):
        opts = 'norequire_active_mds'
        if self.client_keyring_path and self.client_id:
            opts += ',secret=' + self.get_key_from_keyfile()
        if self.config_path:
            opts += ',conf=' + self.config_path
        if self.rbytes:
            opts += ",rbytes"
        else:
            opts += ",norbytes"
        if self.snapdirname != '.snap':
            opts += f',snapdirname={self.snapdirname}'

        mount_cmd = ['sudo'] + self._nsenter_args
        stx_opt = self._make_mount_cmd_old_or_new_style()
        for opt_name, opt_val in stx_opt[1].items():
            opts += f',{opt_name}={opt_val}'
        if mntopts:
            opts += ',' + ','.join(mntopts)
        log.info(f'mounting using device: {stx_opt[0]}')
        # do not fall-back to old-style mount (catch new-style
        # mount syntax bugs in the kernel). exclude this config
        # when using v1-style syntax, since old mount helpers
        # (pre-quincy) would pass this option to the kernel.
        if self.syntax_style != 'v1':
            opts += ",nofallback"
        mount_cmd += self._mount_bin + [stx_opt[0], self.hostfs_mntpt, '-v',
                                        '-o', opts]
        return mount_cmd

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
            log.debug('Killing processes on client.{id}...'.format(id=self.client_id))
            self.client_remote.run(
                args=['sudo', run.Raw('PATH=/usr/sbin:$PATH'), 'lsof',
                      run.Raw(';'), 'ps', 'auxf'],
                timeout=(15*60), omit_sudo=False)
            raise e

        if self.dynamic_debug:
            kmount_count = self.ctx.get(f'kmount_count.{self.client_remote.hostname}')
            assert kmount_count
            if kmount_count == 1:
                self.disable_dynamic_debug()
            self.ctx[f'kmount_count.{self.client_remote.hostname}'] = kmount_count - 1

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
            log.debug('Force/lazy unmounting on client.{id}...'.format(id=self.client_id))
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
                    meta_dir = "{meta_dir}"
                    result = dict()
                    for dir in glob.glob("/sys/kernel/debug/ceph/*"):
                        if os.path.basename(dir) == meta_dir:
                            continue
                        mds_sessions_lines = open(os.path.join(dir, "mds_sessions")).readlines()
                        global_id = mds_sessions_lines[0].split()[1].strip('"')
                        client_id = mds_sessions_lines[1].split()[1].strip('"')
                        result[client_id] = global_id
                    return result
                print(json.dumps(get_id_to_dir()))
            """.format(meta_dir=DEBUGFS_META_DIR))

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

    def _dynamic_debug_control(self, enable):
        """
        Write to dynamic debug control file.
        """
        if enable:
            fdata = "module ceph +p"
        else:
            fdata = "module ceph -p"

        self.run_shell_payload(f"""
sudo modprobe ceph
echo '{fdata}' | sudo tee /sys/kernel/debug/dynamic_debug/control
""")

    def enable_dynamic_debug(self):
        """
        Enable the dynamic debug.
        """
        self._dynamic_debug_control(True)

    def disable_dynamic_debug(self):
        """
        Disable the dynamic debug.
        """
        self._dynamic_debug_control(False)

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

        client_gid = "client%d" % int(self.get_global_id())
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
