import hashlib
import json
import logging
import datetime
import os
import re
import time

from io import StringIO
from contextlib import contextmanager
from textwrap import dedent
from IPy import IP

from teuthology.contextutil import safe_while
from teuthology.misc import get_file, write_file
from teuthology.orchestra import run
from teuthology.orchestra.run import Raw
from teuthology.exceptions import CommandFailedError, ConnectionLostError

from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

UMOUNT_TIMEOUT = 300


class CephFSMountBase(object):
    def __init__(self, ctx, test_dir, client_id, client_remote,
                 client_keyring_path=None, hostfs_mntpt=None,
                 cephfs_name=None, cephfs_mntpt=None, brxnet=None,
                 client_config=None):
        """
        :param test_dir: Global teuthology test dir
        :param client_id: Client ID, the 'foo' in client.foo
        :param client_keyring_path: path to keyring for given client_id
        :param client_remote: Remote instance for the host where client will
                              run
        :param hostfs_mntpt: Path to directory on the FS on which Ceph FS will
                             be mounted
        :param cephfs_name: Name of Ceph FS to be mounted
        :param cephfs_mntpt: Path to directory inside Ceph FS that will be
                             mounted as root
        """
        self.ctx = ctx
        self.test_dir = test_dir

        self._verify_attrs(client_id=client_id,
                           client_keyring_path=client_keyring_path,
                           hostfs_mntpt=hostfs_mntpt, cephfs_name=cephfs_name,
                           cephfs_mntpt=cephfs_mntpt)

        if client_config is None:
            client_config = {}
        self.client_config = client_config

        self.cephfs_name = cephfs_name
        self.client_id = client_id
        self.client_keyring_path = client_keyring_path
        self.client_remote = client_remote
        self.cluster_name = 'ceph' # TODO: use config['cluster']
        self.fs = None

        if cephfs_mntpt is None and client_config.get("mount_path"):
            self.cephfs_mntpt = client_config.get("mount_path")
            log.info(f"using client_config[\"cephfs_mntpt\"] = {self.cephfs_mntpt}")
        else:
            self.cephfs_mntpt = cephfs_mntpt
        log.info(f"cephfs_mntpt = {self.cephfs_mntpt}")

        if hostfs_mntpt is None and client_config.get("mountpoint"):
            self.hostfs_mntpt = client_config.get("mountpoint")
            log.info(f"using client_config[\"hostfs_mntpt\"] = {self.hostfs_mntpt}")
        elif hostfs_mntpt is not None:
            self.hostfs_mntpt = hostfs_mntpt
        else:
            self.hostfs_mntpt = os.path.join(self.test_dir, f'mnt.{self.client_id}')
        self.hostfs_mntpt_dirname = os.path.basename(self.hostfs_mntpt)
        log.info(f"hostfs_mntpt = {self.hostfs_mntpt}")

        self._netns_name = None
        self.nsid = -1
        if brxnet is None:
            self.ceph_brx_net = '192.168.0.0/16'
        else:
            self.ceph_brx_net = brxnet

        self.test_files = ['a', 'b', 'c']

        self.background_procs = []

    # This will cleanup the stale netnses, which are from the
    # last failed test cases.
    @staticmethod
    def cleanup_stale_netnses_and_bridge(remote):
        p = remote.run(args=['ip', 'netns', 'list'],
                       stdout=StringIO(), timeout=(5*60))
        p = p.stdout.getvalue().strip()

        # Get the netns name list
        netns_list = re.findall(r'ceph-ns-[^()\s][-.\w]+[^():\s]', p)

        # Remove the stale netnses
        for ns in netns_list:
            ns_name = ns.split()[0]
            args = ['sudo', 'ip', 'netns', 'delete', '{0}'.format(ns_name)]
            try:
                remote.run(args=args, timeout=(5*60), omit_sudo=False)
            except Exception:
                pass

        # Remove the stale 'ceph-brx'
        try:
            args = ['sudo', 'ip', 'link', 'delete', 'ceph-brx']
            remote.run(args=args, timeout=(5*60), omit_sudo=False)
        except Exception:
            pass

    def _parse_netns_name(self):
        self._netns_name = '-'.join(["ceph-ns",
                                     re.sub(r'/+', "-", self.mountpoint)])

    @property
    def mountpoint(self):
        if self.hostfs_mntpt is None:
            self.hostfs_mntpt = os.path.join(self.test_dir,
                                             self.hostfs_mntpt_dirname)
        return self.hostfs_mntpt

    @mountpoint.setter
    def mountpoint(self, path):
        if not isinstance(path, str):
            raise RuntimeError('path should be of str type.')
        self._mountpoint = self.hostfs_mntpt = path

    @property
    def netns_name(self):
        if self._netns_name == None:
            self._parse_netns_name()
        return self._netns_name

    @netns_name.setter
    def netns_name(self, name):
        self._netns_name = name

    def assert_that_ceph_fs_exists(self):
        output = self.ctx.managers[self.cluster_name].raw_cluster_cmd("fs", "ls")
        if self.cephfs_name:
            assert self.cephfs_name in output, \
                'expected ceph fs is not present on the cluster'
            log.info(f'Mounting Ceph FS {self.cephfs_name}; just confirmed its presence on cluster')
        else:
            assert 'No filesystems enabled' not in output, \
                'ceph cluster has no ceph fs, not even the default ceph fs'
            log.info('Mounting default Ceph FS; just confirmed its presence on cluster')

    def assert_and_log_minimum_mount_details(self):
        """
        Make sure we have minimum details required for mounting. Ideally, this
        method should be called at the beginning of the mount method.
        """
        if not self.client_id or not self.client_remote or \
           not self.hostfs_mntpt:
            log.error(f"self.client_id = {self.client_id}")
            log.error(f"self.client_remote = {self.client_remote}")
            log.error(f"self.hostfs_mntpt = {self.hostfs_mntpt}")
            errmsg = ('Mounting CephFS requires that at least following '
                      'details to be provided -\n'
                      '1. the client ID,\n2. the mountpoint and\n'
                      '3. the remote machine where CephFS will be mounted.\n')
            raise RuntimeError(errmsg)

        self.assert_that_ceph_fs_exists()

        log.info('Mounting Ceph FS. Following are details of mount; remember '
                 '"None" represents Python type None -')
        log.info(f'self.client_remote.hostname = {self.client_remote.hostname}')
        log.info(f'self.client.name = client.{self.client_id}')
        log.info(f'self.hostfs_mntpt = {self.hostfs_mntpt}')
        log.info(f'self.cephfs_name = {self.cephfs_name}')
        log.info(f'self.cephfs_mntpt = {self.cephfs_mntpt}')
        log.info(f'self.client_keyring_path = {self.client_keyring_path}')
        if self.client_keyring_path:
            log.info('keyring content -\n' +
                     get_file(self.client_remote, self.client_keyring_path,
                              sudo=True).decode())

    def is_blocked(self):
        self.fs = Filesystem(self.ctx, name=self.cephfs_name)

        try:
            output = self.fs.get_ceph_cmd_stdout('osd blocklist ls')
        except CommandFailedError:
            # Fallback for older Ceph cluster
            output = self.fs.get_ceph_cmd_stdout('osd blacklist ls')

        return self.addr in output

    def is_stuck(self):
        """
        Check if mount is stuck/in a hanged state.
        """
        if not self.is_mounted():
            return False

        retval = self.client_remote.run(args=f'sudo stat {self.hostfs_mntpt}',
                                        omit_sudo=False, wait=False).returncode
        if retval == 0:
            return False

        time.sleep(10)
        proc = self.client_remote.run(args='ps -ef', stdout=StringIO())
        # if proc was running even after 10 seconds, it has to be stuck.
        if f'stat {self.hostfs_mntpt}' in proc.stdout.getvalue():
            log.critical('client mounted at self.hostfs_mntpt is stuck!')
            return True
        return False

    def is_mounted(self):
        file = self.client_remote.read_file('/proc/self/mounts',stdout=StringIO())
        if self.hostfs_mntpt in file:
            return True
        else:
            log.debug(f"not mounted; /proc/self/mounts is:\n{file}")
            return False

    def setupfs(self, name=None):
        if name is None and self.fs is not None:
            # Previous mount existed, reuse the old name
            name = self.fs.name
        self.fs = Filesystem(self.ctx, name=name)
        log.info('Wait for MDS to reach steady state...')
        self.fs.wait_for_daemons()
        log.info('Ready to start {}...'.format(type(self).__name__))

    def _create_mntpt(self):
        self.client_remote.run(args=f'mkdir -p -v {self.hostfs_mntpt}',
                               timeout=60)
        # Use 0000 mode to prevent undesired modifications to the mountpoint on
        # the local file system.
        self.client_remote.run(args=f'chmod 0000 {self.hostfs_mntpt}',
                               timeout=60)

    @property
    def _nsenter_args(self):
        return ['nsenter', f'--net=/var/run/netns/{self.netns_name}']

    def _set_filemode_on_mntpt(self):
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

    def _setup_brx_and_nat(self):
        # The ip for ceph-brx should be
        ip = IP(self.ceph_brx_net)[-2]
        mask = self.ceph_brx_net.split('/')[1]
        brd = IP(self.ceph_brx_net).broadcast()

        brx = self.client_remote.run(args=['ip', 'addr'], stderr=StringIO(),
                                     stdout=StringIO(), timeout=(5*60))
        brx = re.findall(r'inet .* ceph-brx', brx.stdout.getvalue())
        if brx:
            # If the 'ceph-brx' already exists, then check whether
            # the new net is conflicting with it
            _ip, _mask = brx[0].split()[1].split('/', 1)
            if _ip != "{}".format(ip) or _mask != mask:
                raise RuntimeError("Conflict with existing ceph-brx {0}, new {1}/{2}".format(brx[0].split()[1], ip, mask))

        # Setup the ceph-brx and always use the last valid IP
        if not brx:
            log.info("Setuping the 'ceph-brx' with {0}/{1}".format(ip, mask))

            self.run_shell_payload(f"""
                set -e
                sudo ip link add name ceph-brx type bridge
                sudo ip addr flush dev ceph-brx
                sudo ip link set ceph-brx up
                sudo ip addr add {ip}/{mask} brd {brd} dev ceph-brx
            """, timeout=(5*60), omit_sudo=False, cwd='/')
        
        args = "echo 1 | sudo tee /proc/sys/net/ipv4/ip_forward"
        self.client_remote.run(args=args, timeout=(5*60), omit_sudo=False)
        
        # Setup the NAT
        p = self.client_remote.run(args=['route'], stderr=StringIO(),
                                   stdout=StringIO(), timeout=(5*60))
        p = re.findall(r'default .*', p.stdout.getvalue())
        if p == False:
            raise RuntimeError("No default gw found")
        gw = p[0].split()[7]

        self.run_shell_payload(f"""
            set -e
            sudo iptables -A FORWARD -o {gw} -i ceph-brx -j ACCEPT
            sudo iptables -A FORWARD -i {gw} -o ceph-brx -j ACCEPT
            sudo iptables -t nat -A POSTROUTING -s {ip}/{mask} -o {gw} -j MASQUERADE
        """, timeout=(5*60), omit_sudo=False, cwd='/')

    def _setup_netns(self):
        p = self.client_remote.run(args=['ip', 'netns', 'list'],
                                   stderr=StringIO(), stdout=StringIO(),
                                   timeout=(5*60)).stdout.getvalue().strip()

        # Get the netns name list
        netns_list = re.findall(r'[^()\s][-.\w]+[^():\s]', p)

        out = re.search(r"{0}".format(self.netns_name), p)
        if out is None:
            # Get an uniq nsid for the new netns
            nsid = 0
            p = self.client_remote.run(args=['ip', 'netns', 'list-id'],
                                       stderr=StringIO(), stdout=StringIO(),
                                       timeout=(5*60)).stdout.getvalue()
            while True:
                out = re.search(r"nsid {} ".format(nsid), p)
                if out is None:
                    break

                nsid += 1

            # Add one new netns and set it id
            self.run_shell_payload(f"""
                set -e
                sudo ip netns add {self.netns_name}
                sudo ip netns set {self.netns_name} {nsid}
            """, timeout=(5*60), omit_sudo=False, cwd='/')
            self.nsid = nsid;
        else:
            # The netns already exists and maybe suspended by self.kill()
            self.resume_netns();

            nsid = int(re.search(r"{0} \(id: (\d+)\)".format(self.netns_name), p).group(1))
            self.nsid = nsid;
            return

        # Get one ip address for netns
        ips = IP(self.ceph_brx_net)
        for ip in ips:
            found = False
            if ip == ips[0]:
                continue
            if ip == ips[-2]:
                raise RuntimeError("we have ran out of the ip addresses")

            for ns in netns_list:
                ns_name = ns.split()[0]
                args = ['sudo', 'ip', 'netns', 'exec', '{0}'.format(ns_name), 'ip', 'addr']
                try:
                    p = self.client_remote.run(args=args, stderr=StringIO(),
                                               stdout=StringIO(), timeout=(5*60),
                                               omit_sudo=False)
                    q = re.search("{0}".format(ip), p.stdout.getvalue())
                    if q is not None:
                        found = True
                        break
                except CommandFailedError:
                    if "No such file or directory" in p.stderr.getvalue():
                        pass
                    if "Invalid argument" in p.stderr.getvalue():
                        pass

            if found == False:
                break

        mask = self.ceph_brx_net.split('/')[1]
        brd = IP(self.ceph_brx_net).broadcast()

        log.info("Setuping the netns '{0}' with {1}/{2}".format(self.netns_name, ip, mask))

        # Setup the veth interfaces
        brxip = IP(self.ceph_brx_net)[-2]
        self.run_shell_payload(f"""
            set -e
            sudo ip link add veth0 netns {self.netns_name} type veth peer name brx.{nsid}
            sudo ip netns exec {self.netns_name} ip addr add {ip}/{mask} brd {brd} dev veth0
            sudo ip netns exec {self.netns_name} ip link set veth0 up
            sudo ip netns exec {self.netns_name} ip link set lo up
            sudo ip netns exec {self.netns_name} ip route add default via {brxip}
        """, timeout=(5*60), omit_sudo=False, cwd='/')

        # Bring up the brx interface and join it to 'ceph-brx'
        self.run_shell_payload(f"""
            set -e
            sudo ip link set brx.{nsid} up
            sudo ip link set dev brx.{nsid} master ceph-brx
        """, timeout=(5*60), omit_sudo=False, cwd='/')

    def _cleanup_netns(self):
        if self.nsid == -1:
            return
        log.info("Removing the netns '{0}'".format(self.netns_name))

        # Delete the netns and the peer veth interface
        self.run_shell_payload(f"""
            set -e
            sudo ip link set brx.{self.nsid} down
            sudo ip link delete dev brx.{self.nsid}
            sudo ip netns delete {self.netns_name}
        """, timeout=(5*60), omit_sudo=False, cwd='/')

        self.nsid = -1

    def _cleanup_brx_and_nat(self):
        brx = self.client_remote.run(args=['ip', 'addr'], stderr=StringIO(),
                                     stdout=StringIO(), timeout=(5*60))
        brx = re.findall(r'inet .* ceph-brx', brx.stdout.getvalue())
        if not brx:
            return

        # If we are the last netns, will delete the ceph-brx
        args = ['sudo', 'ip', 'link', 'show']
        p = self.client_remote.run(args=args, stdout=StringIO(),
                                   timeout=(5*60), omit_sudo=False)
        _list = re.findall(r'brx\.', p.stdout.getvalue().strip())
        if len(_list) != 0:
            return

        log.info("Removing the 'ceph-brx'")

        self.run_shell_payload("""
            set -e
            sudo ip link set ceph-brx down
            sudo ip link delete ceph-brx
        """, timeout=(5*60), omit_sudo=False, cwd='/')

        # Drop the iptables NAT rules
        ip = IP(self.ceph_brx_net)[-2]
        mask = self.ceph_brx_net.split('/')[1]

        p = self.client_remote.run(args=['route'], stderr=StringIO(),
                                   stdout=StringIO(), timeout=(5*60))
        p = re.findall(r'default .*', p.stdout.getvalue())
        if p == False:
            raise RuntimeError("No default gw found")
        gw = p[0].split()[7]
        self.run_shell_payload(f"""
            set -e
            sudo iptables -D FORWARD -o {gw} -i ceph-brx -j ACCEPT
            sudo iptables -D FORWARD -i {gw} -o ceph-brx -j ACCEPT
            sudo iptables -t nat -D POSTROUTING -s {ip}/{mask} -o {gw} -j MASQUERADE
        """, timeout=(5*60), omit_sudo=False, cwd='/')

    def setup_netns(self):
        """
        Setup the netns for the mountpoint.
        """
        log.info("Setting the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))
        self._setup_brx_and_nat()
        self._setup_netns()

    def cleanup_netns(self):
        """
        Cleanup the netns for the mountpoint.
        """
        # We will defer cleaning the netnses and bridge until the last
        # mountpoint is unmounted, this will be a temporary work around
        # for issue#46282.

        # log.info("Cleaning the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))
        # self._cleanup_netns()
        # self._cleanup_brx_and_nat()

    def suspend_netns(self):
        """
        Suspend the netns veth interface.
        """
        if self.nsid == -1:
            return

        log.info("Suspending the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))

        args = ['sudo', 'ip', 'link', 'set', 'brx.{0}'.format(self.nsid), 'down']
        self.client_remote.run(args=args, timeout=(5*60), omit_sudo=False)

    def resume_netns(self):
        """
        Resume the netns veth interface.
        """
        if self.nsid == -1:
            return

        log.info("Resuming the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))

        args = ['sudo', 'ip', 'link', 'set', 'brx.{0}'.format(self.nsid), 'up']
        self.client_remote.run(args=args, timeout=(5*60), omit_sudo=False)

    def mount(self, mntopts=[], check_status=True, **kwargs):
        """
        kwargs expects its members to be same as the arguments accepted by
        self.update_attrs().
        """
        raise NotImplementedError()

    def mount_wait(self, **kwargs):
        """
        Accepts arguments same as self.mount().
        """
        self.mount(**kwargs)
        self.wait_until_mounted()

    def _run_umount_lf(self):
        log.debug(f'Force/lazy unmounting on client.{self.client_id}')

        try:
            proc = self.client_remote.run(
                args=f'sudo umount --lazy --force {self.hostfs_mntpt}',
                timeout=UMOUNT_TIMEOUT, omit_sudo=False)
        except CommandFailedError:
            if self.is_mounted():
                raise

        return proc

    def umount(self):
        raise NotImplementedError()

    def umount_wait(self, force=False, require_clean=False,
                    timeout=UMOUNT_TIMEOUT):
        """

        :param force: Expect that the mount will not shutdown cleanly: kill
                      it hard.
        :param require_clean: Wait for the Ceph client associated with the
                              mount (e.g. ceph-fuse) to terminate, and
                              raise if it doesn't do so cleanly.
        :param timeout: amount of time to be waited for umount command to finish
        :return:
        """
        raise NotImplementedError()

    def _verify_attrs(self, **kwargs):
        """
        Verify that client_id, client_keyring_path, client_remote, hostfs_mntpt,
        cephfs_name, cephfs_mntpt are either type str or None.
        """
        for k, v in kwargs.items():
            if v is not None and not isinstance(v, str):
                raise RuntimeError('value of attributes should be either str '
                                   f'or None. {k} - {v}')

    def update_attrs(self, **kwargs):
        verify_keys = [
          'client_id',
          'client_keyring_path',
          'hostfs_mntpt',
          'cephfs_name',
          'cephfs_mntpt',
        ]

        self._verify_attrs(**{key: kwargs[key] for key in verify_keys if key in kwargs})

        for k in verify_keys:
            v = kwargs.get(k)
            if v is not None:
                setattr(self, k, v)

    def remount(self, **kwargs):
        """
        Update mount object's attributes and attempt remount with these
        new values for these attrbiutes.

        1. Run umount_wait().
        2. Run update_attrs().
        3. Run mount().

        Accepts arguments of self.mount() and self.update_attrs() with 1
        exception: wait accepted too which can be True or False.
        """
        self.umount_wait()
        assert not self.is_mounted()

        mntopts = kwargs.pop('mntopts', [])
        check_status = kwargs.pop('check_status', True)
        wait = kwargs.pop('wait', True)

        self.update_attrs(**kwargs)

        retval = self.mount(mntopts=mntopts, check_status=check_status, **kwargs)
        # avoid this scenario (again): mount command might've failed and
        # check_status might have silenced the exception, yet we attempt to
        # wait which might lead to an error.
        if retval is None and wait:
            self.wait_until_mounted()

        return retval

    def kill(self):
        """
        Suspend the netns veth interface to make the client disconnected
        from the ceph cluster
        """
        log.info('Killing connection on {0}...'.format(self.client_remote.name))
        self.suspend_netns()

    def kill_cleanup(self):
        """
        Follow up ``kill`` to get to a clean unmounted state.
        """
        log.info('Cleaning up killed connection on {0}'.format(self.client_remote.name))
        self.umount_wait(force=True)

    def cleanup(self):
        """
        Remove the mount point.

        Prerequisite: the client is not mounted.
        """
        log.info('Cleaning up mount {0}'.format(self.client_remote.name))
        stderr = StringIO()
        try:
            self.client_remote.run(args=['rmdir', '--', self.mountpoint],
                                   cwd=self.test_dir, stderr=stderr,
                                   timeout=(60*5), check_status=False)
        except CommandFailedError:
            if "no such file or directory" not in stderr.getvalue().lower():
                raise

        self.cleanup_netns()

    def wait_until_mounted(self):
        raise NotImplementedError()

    def get_keyring_path(self):
        # N.B.: default keyring is /etc/ceph/ceph.keyring; see ceph.py and generate_caps
        return '/etc/ceph/ceph.client.{id}.keyring'.format(id=self.client_id)

    def get_key_from_keyfile(self):
        # XXX: don't call run_shell(), since CephFS might be unmounted.
        keyring = self.client_remote.read_file(self.client_keyring_path).\
            decode()

        for line in keyring.split('\n'):
            if line.find('key') != -1:
                return line[line.find('=') + 1 : ].strip()

        raise RuntimeError('Key not found in keyring file '
                           f'{self.client_keyring_path}. Its contents are -\n'
                           f'{keyring}')

    @property
    def config_path(self):
        """
        Path to ceph.conf: override this if you're not a normal systemwide ceph install
        :return: stringv
        """
        return "/etc/ceph/ceph.conf"

    @contextmanager
    def mounted_wait(self):
        """
        A context manager, from an initially unmounted state, to mount
        this, yield, and then unmount and clean up.
        """
        self.mount()
        self.wait_until_mounted()
        try:
            yield
        finally:
            self.umount_wait()

    def create_file(self, filename='testfile', dirname=None, user=None,
                    check_status=True):
        assert(self.is_mounted())

        if not os.path.isabs(filename):
            if dirname:
                if os.path.isabs(dirname):
                    path = os.path.join(dirname, filename)
                else:
                    path = os.path.join(self.hostfs_mntpt, dirname, filename)
            else:
                path = os.path.join(self.hostfs_mntpt, filename)
        else:
            path = filename

        if user:
            args = ['sudo', '-u', user, '-s', '/bin/bash', '-c', 'touch ' + path]
        else:
            args = 'touch ' + path

        return self.client_remote.run(args=args, check_status=check_status)

    def create_files(self):
        assert(self.is_mounted())

        for suffix in self.test_files:
            log.info("Creating file {0}".format(suffix))
            self.client_remote.run(args=[
                'touch', os.path.join(self.hostfs_mntpt, suffix)
            ])

    def test_create_file(self, filename='testfile', dirname=None, user=None,
                         check_status=True):
        return self.create_file(filename=filename, dirname=dirname, user=user,
                                check_status=False)

    def check_files(self):
        assert(self.is_mounted())

        for suffix in self.test_files:
            log.info("Checking file {0}".format(suffix))
            r = self.client_remote.run(args=[
                'ls', os.path.join(self.hostfs_mntpt, suffix)
            ], check_status=False)
            if r.exitstatus != 0:
                raise RuntimeError("Expected file {0} not found".format(suffix))

    def write_file(self, path, data, perms=None):
        """
        Write the given data at the given path and set the given perms to the
        file on the path.
        """
        if path.find(self.hostfs_mntpt) == -1:
            path = os.path.join(self.hostfs_mntpt, path)

        write_file(self.client_remote, path, data)

        if perms:
            self.run_shell(args=f'chmod {perms} {path}')

    def read_file(self, path, sudo=False):
        """
        Return the data from the file on given path.
        """
        if path.find(self.hostfs_mntpt) == -1:
            path = os.path.join(self.hostfs_mntpt, path)

        args = []
        if sudo:
            args.append('sudo')
        args += ['cat', path]

        return self.run_shell(args=args, omit_sudo=False).stdout.getvalue().strip()

    def create_destroy(self):
        assert(self.is_mounted())

        filename = "{0} {1}".format(datetime.datetime.now(), self.client_id)
        log.debug("Creating test file {0}".format(filename))
        self.client_remote.run(args=[
            'touch', os.path.join(self.hostfs_mntpt, filename)
        ])
        log.debug("Deleting test file {0}".format(filename))
        self.client_remote.run(args=[
            'rm', '-f', os.path.join(self.hostfs_mntpt, filename)
        ])

    def _run_python(self, pyscript, py_version='python3', sudo=False, timeout=None):
        args, omit_sudo = [], True
        if sudo:
            args.append('sudo')
            omit_sudo = False
        timeout_args = ['--timeout', "%d" % timeout] if timeout is not None else []
        args += ['stdin-killer', *timeout_args, '--', py_version, '-c', pyscript]
        return self.client_remote.run(args=args, wait=False, stdin=run.PIPE,
                                      stdout=StringIO(), omit_sudo=omit_sudo)

    def run_python(self, pyscript, py_version='python3', sudo=False, timeout=None):
        p = self._run_python(pyscript, py_version, sudo=sudo, timeout=timeout)
        p.wait()
        return p.stdout.getvalue().strip()

    def run_shell(self, args, **kwargs):
        kwargs.setdefault('cwd', self.mountpoint)
        kwargs.setdefault('omit_sudo', False)
        kwargs.setdefault('stdout', StringIO())
        kwargs.setdefault('stderr', StringIO())
        kwargs.setdefault('timeout', 300)

        return self.client_remote.run(args=args, **kwargs)

    def run_shell_payload(self, payload, wait=True, timeout=900, **kwargs):
        kwargs.setdefault('cwd', self.mountpoint)
        kwargs.setdefault('omit_sudo', False)
        kwargs.setdefault('stdout', StringIO())
        kwargs.setdefault('stderr', StringIO())
        kwargs.setdefault('stdin', run.PIPE)
        args = []
        if kwargs.pop('sudo', False):
            args.append('sudo')
            kwargs['omit_sudo'] = False
        args.append("stdin-killer")
        if timeout is not None:
            args.append(f"--timeout={timeout}")
        args += ("--", "bash", "-c", Raw(f"'{payload}'"))
        p = self.client_remote.run(args=args, wait=False, **kwargs)
        if wait:
            p.stdin.close()
            p.wait()
        return p

    def run_as_user(self, **kwargs):
        """
        Besides the arguments defined for run_shell() this method also
        accepts argument 'user'.
        """
        args = kwargs.pop('args')
        user = kwargs.pop('user')
        if isinstance(args, str):
            args = ['sudo', '-u', user, '-s', '/bin/bash', '-c', args]
        elif isinstance(args, list):
            cmdlist = args
            cmd = ''
            for i in cmdlist:
                cmd = cmd + i + ' '
            # get rid of extra space at the end.
            cmd = cmd[:-1]

            args = ['sudo', '-u', user, '-s', '/bin/bash', '-c', cmd]

        kwargs['args'] = args
        kwargs['omit_sudo'] = False
        return self.run_shell(**kwargs)

    def run_as_root(self, **kwargs):
        """
        Accepts same arguments as run_shell().
        """
        kwargs['user'] = 'root'
        return self.run_as_user(**kwargs)

    def assert_retval(self, proc_retval, exp_retval):
        msg = (f'expected return value: {exp_retval}\n'
               f'received return value: {proc_retval}\n')
        assert proc_retval == exp_retval, msg

    def _verify(self, proc, exp_retval=None, exp_errmsgs=None):
        if exp_retval is None and exp_errmsgs is None:
            raise RuntimeError('Method didn\'t get enough parameters. Pass '
                               'return value or error message expected from '
                               'the command/process.')

        if exp_retval is not None:
            self.assert_retval(proc.returncode, exp_retval)
        if exp_errmsgs is None:
            return

        if isinstance(exp_errmsgs, str):
            exp_errmsgs = (exp_errmsgs, )

        proc_stderr = proc.stderr.getvalue().lower()
        msg = ('didn\'t find any of the expected string in stderr.\n'
               f'expected string: {exp_errmsgs}\n'
               f'received error message: {proc_stderr}\n'
               'note: received error message is converted to lowercase')
        for e in exp_errmsgs:
            if e in proc_stderr:
                break
        # this else is meant for for loop.
        else:
            assert False, msg

    def negtestcmd(self, args, retval=None, errmsgs=None, stdin=None,
                   cwd=None, wait=True):
        """
        Conduct a negative test for the given command.

        retval and errmsgs are parameters to confirm the cause of command
        failure.

        Note: errmsgs is expected to be a tuple, but in case there's only
        error message, it can also be a string. This method will handle
        that internally.
        """
        proc = self.run_shell(args=args, wait=wait, stdin=stdin, cwd=cwd,
                              check_status=False)
        self._verify(proc, retval, errmsgs)
        return proc

    def negtestcmd_as_user(self, args, user, retval=None, errmsgs=None,
                           stdin=None, cwd=None, wait=True):
        proc = self.run_as_user(args=args, user=user, wait=wait, stdin=stdin,
                                cwd=cwd, check_status=False)
        self._verify(proc, retval, errmsgs)
        return proc

    def negtestcmd_as_root(self, args, retval=None, errmsgs=None, stdin=None,
                           cwd=None, wait=True):
        proc = self.run_as_root(args=args, wait=wait, stdin=stdin, cwd=cwd,
                                check_status=False)
        self._verify(proc, retval, errmsgs)
        return proc

    def open_for_reading(self, basename):
        """
        Open a file for reading only.
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        return self._run_python(dedent(
            """
            import os
            mode = os.O_RDONLY
            fd = os.open("{path}", mode)
            os.close(fd)
            """.format(path=path)
        ))

    def open_for_writing(self, basename, creat=True, trunc=True, excl=False):
        """
        Open a file for writing only.
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        return self._run_python(dedent(
            """
            import os
            mode = os.O_WRONLY
            if {creat}:
                mode |= os.O_CREAT
            if {trunc}:
                mode |= os.O_TRUNC
            if {excl}:
                mode |= os.O_EXCL
            fd = os.open("{path}", mode)
            os.close(fd)
            """.format(path=path, creat=creat, trunc=trunc, excl=excl)
        ))

    def open_no_data(self, basename):
        """
        A pure metadata operation
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        p = self._run_python(dedent(
            """
            f = open("{path}", 'w')
            """.format(path=path)
        ))
        p.wait()

    def open_background(self, basename="background_file", write=True, content="content"):
        """
        Open a file for writing, then block such that the client
        will hold a capability.

        Don't return until the remote process has got as far as opening
        the file, then return the RemoteProcess instance.
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        if write:
            pyscript = dedent("""
                import fcntl
                import os
                import sys
                import time

                fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

                with open("{path}", 'w') as f:
                    f.write("{content}")
                    f.flush()
                    while True:
                        print("open_background: keeping file open", file=sys.stderr)
                        try:
                             if os.read(0, 4096) == b"":
                                  break
                        except BlockingIOError:
                            pass
                        time.sleep(2)
                """).format(path=path, content=content)
        else:
            pyscript = dedent("""
                import fcntl
                import os
                import sys
                import time

                fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

                with open("{path}", 'r') as f:
                    while True:
                        print("open_background: keeping file open", file=sys.stderr)
                        try:
                             if os.read(0, 4096) == b"":
                                  break
                        except BlockingIOError:
                            pass
                        time.sleep(2)
                """).format(path=path)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)

        # This wait would not be sufficient if the file had already
        # existed, but it's simple and in practice users of open_background
        # are not using it on existing files.
        if write:
            self.wait_for_visible(basename, size=len(content))
        else:
            self.wait_for_visible(basename)

        return rproc

    def open_dir_background(self, basename):
        """
        Create and hold a capability to a directory.
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        pyscript = dedent("""
            import fcntl
            import sys
            import time
            import os

            fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

            os.mkdir("{path}")
            fd = os.open("{path}", os.O_RDONLY)
            while True:
                print("open_dir_background: keeping dir open", file=sys.stderr)
                try:
                     if os.read(0, 4096) == b"":
                          break
                except BlockingIOError:
                    pass
                time.sleep(2)
            """).format(path=path)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)

        self.wait_for_visible(basename)

        return rproc

    def wait_for_dir_empty(self, dirname, timeout=30):
        dirpath = os.path.join(self.hostfs_mntpt, dirname)
        with safe_while(sleep=5, tries=(timeout//5)) as proceed:
            while proceed():
                p = self.run_shell_payload(f"stat -c %h {dirpath}")
                nr_links = int(p.stdout.getvalue().strip())
                if nr_links == 2:
                    return

    def wait_for_visible(self, basename="background_file", size=None, timeout=30):
        i = 0
        args = ['stat']
        if size is not None:
            args += ['--printf=%s']
        args += [os.path.join(self.hostfs_mntpt, basename)]
        while i < timeout:
            p = self.client_remote.run(args=args, stdout=StringIO(), check_status=False)
            if p.exitstatus == 0:
                if size is not None:
                    s = p.stdout.getvalue().strip()
                    if int(s) == size:
                        log.info(f"File {basename} became visible with size {size} from {self.client_id} after {i}s")
                        return
                    else:
                        log.error(f"File {basename} became visible but with size {int(s)} not {size}")
                else:
                    log.info(f"File {basename} became visible from {self.client_id} after {i}s")
                    return
            time.sleep(1)
            i += 1

        raise RuntimeError("Timed out after {0}s waiting for {1} to become visible from {2}".format(
            i, basename, self.client_id))

    def lock_background(self, basename="background_file", do_flock=True):
        """
        Open and lock a files for writing, hold the lock in a background process
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        script_builder = """
            import sys
            import time
            import fcntl
            import os
            import struct

            fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
        """
        if do_flock:
            script_builder += """
            f1 = open("{path}-1", 'w')
            fcntl.flock(f1, fcntl.LOCK_EX | fcntl.LOCK_NB)
            """
        script_builder += """
            f2 = open("{path}-2", 'w')
            lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
            fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            while True:
                print("lock_background: keeping lock", file=sys.stderr)
                try:
                     if os.read(0, 4096) == b"":
                          break
                except BlockingIOError:
                    pass
                time.sleep(2)
            """

        pyscript = dedent(script_builder).format(path=path)

        log.info("lock_background file {0}".format(basename))
        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def lock_and_release(self, basename="background_file"):
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        script = """
            import time
            import fcntl
            import struct
            import sys
            f1 = open("{path}-1", 'w')
            fcntl.flock(f1, fcntl.LOCK_EX)
            f2 = open("{path}-2", 'w')
            lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
            fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            """
        pyscript = dedent(script).format(path=path)

        log.info("lock_and_release file {0}".format(basename))
        return self._run_python(pyscript)

    def check_filelock(self, basename="background_file", do_flock=True):
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        script_builder = """
            import fcntl
            import errno
            import struct
            import sys
        """
        if do_flock:
            script_builder += """
            f1 = open("{path}-1", 'r')
            try:
                fcntl.flock(f1, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    pass
            else:
                raise RuntimeError("flock on file {path}-1 not found")"""
        script_builder += """
            f2 = open("{path}-2", 'r')
            try:
                lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
                fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    pass
            else:
                raise RuntimeError("posix lock on file {path}-2 not found")
            """
        pyscript = dedent(script_builder).format(path=path)

        log.info("check lock on file {0}".format(basename))
        self.client_remote.run(args=[
            'python3', '-c', pyscript
        ])

    def write_background(self, basename="background_file", loop=False):
        """
        Open a file for writing, complete as soon as you can
        :param basename:
        :return:
        """
        assert(self.is_mounted())

        path = os.path.join(self.hostfs_mntpt, basename)

        pyscript = dedent("""
            import fcntl
            import os
            import sys
            import time

            fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

            fd = os.open("{path}", os.O_RDWR | os.O_CREAT, 0o644)
            try:
                while True:
                    print("write_background: writing", file=sys.stderr)
                    os.write(fd, b'content')
                    if not {loop}:
                        break
                    try:
                        if os.read(0, 4096) == b"":
                            break
                    except BlockingIOError:
                        pass
                    time.sleep(2)
            except IOError as e:
                pass
            os.close(fd)
            """).format(path=path, loop=str(loop))

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def write_n_mb(self, filename, n_mb, seek=0, wait=True):
        """
        Write the requested number of megabytes to a file
        """
        assert(self.is_mounted())

        return self.run_shell(["dd", "if=/dev/urandom", "of={0}".format(filename),
                               "bs=1M", "conv=fdatasync",
                               "count={0}".format(int(n_mb)),
                               "seek={0}".format(int(seek))
                               ], wait=wait)

    def write_test_pattern(self, filename, size):
        log.info("Writing {0} bytes to {1}".format(size, filename))
        return self.run_python(dedent("""
            import zlib
            path = "{path}"
            with open(path, 'w') as f:
                for i in range(0, {size}):
                    val = zlib.crc32(str(i).encode('utf-8')) & 7
                    f.write(chr(val))
        """.format(
            path=os.path.join(self.hostfs_mntpt, filename),
            size=size
        )))

    def validate_test_pattern(self, filename, size, timeout=None):
        log.info("Validating {0} bytes from {1}".format(size, filename))
        # Use sudo because cephfs-data-scan may recreate the file with owner==root
        return self.run_python(dedent("""
            import zlib
            path = "{path}"
            with open(path, 'r') as f:
                bytes = f.read()
            if len(bytes) != {size}:
                raise RuntimeError("Bad length {{0}} vs. expected {{1}}".format(
                    len(bytes), {size}
                ))
            for i, b in enumerate(bytes):
                val = zlib.crc32(str(i).encode('utf-8')) & 7
                if b != chr(val):
                    raise RuntimeError("Bad data at offset {{0}}".format(i))
        """.format(
            path=os.path.join(self.hostfs_mntpt, filename),
            size=size
        )), sudo=True, timeout=timeout)

    def open_n_background(self, fs_path, count):
        """
        Open N files for writing, hold them open in a background process

        :param fs_path: Path relative to CephFS root, e.g. "foo/bar"
        :return: a RemoteProcess
        """
        assert(self.is_mounted())

        abs_path = os.path.join(self.hostfs_mntpt, fs_path)

        pyscript = dedent("""
            import fcntl
            import sys
            import time
            import os

            fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

            n = {count}
            abs_path = "{abs_path}"

            if not os.path.exists(abs_path):
                os.makedirs(abs_path)

            handles = []
            for i in range(0, n):
                fname = "file_"+str(i)
                path = os.path.join(abs_path, fname)
                handles.append(open(path, 'w'))

            print("waiting with handles open", file=sys.stderr)
            while True:
                print("open_n_background: keeping files open", file=sys.stderr)
                try:
                     if os.read(0, 4096) == b"":
                          break
                except BlockingIOError:
                    pass
                time.sleep(2)
            print("stdin closed, goodbye!", file=sys.stderr)
            """).format(abs_path=abs_path, count=count)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def create_n_files(self, fs_path, count, sync=False, dirsync=False,
                       unlink=False, finaldirsync=False, hard_links=0):
        """
        Create n files.

        :param sync: sync the file after writing
        :param dirsync: sync the containing directory after closing the file
        :param unlink: unlink the file after closing
        :param finaldirsync: sync the containing directory after closing the last file
        :param hard_links: create given number of hard link(s) for each file
        """

        assert(self.is_mounted())

        abs_path = os.path.join(self.hostfs_mntpt, fs_path)

        pyscript = dedent(f"""
            import os
            import uuid

            n = {count}
            create_hard_links = False
            if {hard_links} > 0:
                create_hard_links = True
            path = "{abs_path}"

            dpath = os.path.dirname(path)
            fnameprefix = os.path.basename(path)
            os.makedirs(dpath, exist_ok=True)

            try:
                dirfd = os.open(dpath, os.O_DIRECTORY)

                for i in range(n):
                    fpath = os.path.join(dpath, f"{{fnameprefix}}_{{i}}")
                    with open(fpath, 'w') as f:
                        f.write(f"{{i}}")
                        if {sync}:
                            f.flush()
                            os.fsync(f.fileno())
                    if {unlink}:
                        os.unlink(fpath)
                    if {dirsync}:
                        os.fsync(dirfd)
                    if create_hard_links:
                        for j in range({hard_links}):
                            os.system(f"ln {{fpath}} {{dpath}}/{{fnameprefix}}_{{i}}_{{uuid.uuid4()}}")     
                if {finaldirsync}:
                    os.fsync(dirfd)
            finally:
                os.close(dirfd)
            """)

        self.run_python(pyscript)

    def teardown(self):
        log.info("Terminating background process")
        self.kill_background()

        if self.is_mounted():
            self.umount()

    def _kill_background(self, p):
        if p.stdin:
            p.stdin.close()
            try:
                p.wait()
            except (CommandFailedError, ConnectionLostError):
                pass

    def kill_background(self, p=None):
        """
        For a process that was returned by one of the _background member functions,
        kill it hard.
        """
        procs = [p] if p is not None else list(self.background_procs)
        for p in procs:
            log.debug(f"terminating {p}")
            self._kill_background(p)
            self.background_procs.remove(p)

    def send_signal(self, signal):
        signal = signal.lower()
        if signal.lower() not in ['sigstop', 'sigcont', 'sigterm', 'sigkill']:
            raise NotImplementedError

        self.client_remote.run(args=['sudo', 'kill', '-{0}'.format(signal),
                                self.client_pid], omit_sudo=False)

    def get_global_id(self):
        raise NotImplementedError()

    def get_global_inst(self):
        raise NotImplementedError()

    def get_global_addr(self):
        raise NotImplementedError()

    def get_osd_epoch(self):
        raise NotImplementedError()

    def get_op_read_count(self):
        raise NotImplementedError()

    def readlink(self, fs_path):
        abs_path = os.path.join(self.hostfs_mntpt, fs_path)

        pyscript = dedent("""
            import os

            print(os.readlink("{path}"))
            """).format(path=abs_path)

        proc = self._run_python(pyscript)
        proc.wait()
        return str(proc.stdout.getvalue().strip())


    def lstat(self, fs_path, follow_symlinks=False, wait=True):
        return self.stat(fs_path, follow_symlinks=False, wait=True)

    def stat(self, fs_path, follow_symlinks=True, wait=True, **kwargs):
        """
        stat a file, and return the result as a dictionary like this:
        {
          "st_ctime": 1414161137.0,
          "st_mtime": 1414161137.0,
          "st_nlink": 33,
          "st_gid": 0,
          "st_dev": 16777218,
          "st_size": 1190,
          "st_ino": 2,
          "st_uid": 0,
          "st_mode": 16877,
          "st_atime": 1431520593.0
        }

        Raises exception on absent file.
        """
        abs_path = os.path.join(self.hostfs_mntpt, fs_path)
        if follow_symlinks:
            stat_call = "os.stat('" + abs_path + "')"
        else:
            stat_call = "os.lstat('" + abs_path + "')"

        pyscript = dedent("""
            import os
            import stat
            import json
            import sys

            try:
                s = {stat_call}
            except OSError as e:
                sys.exit(e.errno)

            attrs = ["st_mode", "st_ino", "st_dev", "st_nlink", "st_uid", "st_gid", "st_size", "st_atime", "st_mtime", "st_ctime"]
            print(json.dumps(
                dict([(a, getattr(s, a)) for a in attrs]),
                indent=2))
            """).format(stat_call=stat_call)
        proc = self._run_python(pyscript, **kwargs)
        if wait:
            proc.wait()
            return json.loads(proc.stdout.getvalue().strip())
        else:
            return proc

    def touch(self, fs_path):
        """
        Create a dentry if it doesn't already exist.  This python
        implementation exists because the usual command line tool doesn't
        pass through error codes like EIO.

        :param fs_path:
        :return:
        """
        abs_path = os.path.join(self.hostfs_mntpt, fs_path)
        pyscript = dedent("""
            import sys
            import errno

            try:
                f = open("{path}", "w")
                f.close()
            except IOError as e:
                sys.exit(errno.EIO)
            """).format(path=abs_path)
        proc = self._run_python(pyscript)
        proc.wait()

    def path_to_ino(self, fs_path, follow_symlinks=True):
        abs_path = os.path.join(self.hostfs_mntpt, fs_path)

        if follow_symlinks:
            pyscript = dedent("""
                import os
                import stat

                print(os.stat("{path}").st_ino)
                """).format(path=abs_path)
        else:
            pyscript = dedent("""
                import os
                import stat

                print(os.lstat("{path}").st_ino)
                """).format(path=abs_path)

        proc = self._run_python(pyscript)
        proc.wait()
        return int(proc.stdout.getvalue().strip())

    def path_to_nlink(self, fs_path):
        abs_path = os.path.join(self.hostfs_mntpt, fs_path)

        pyscript = dedent("""
            import os
            import stat

            print(os.stat("{path}").st_nlink)
            """).format(path=abs_path)

        proc = self._run_python(pyscript)
        proc.wait()
        return int(proc.stdout.getvalue().strip())

    def ls(self, path=None, **kwargs):
        """
        Wrap ls: return a list of strings
        """
        kwargs['args'] = ["ls"]
        if path:
            kwargs['args'].append(path)
        if kwargs.pop('sudo', False):
            kwargs['args'].insert(0, 'sudo')
            kwargs['omit_sudo'] = False
        ls_text = self.run_shell(**kwargs).stdout.getvalue().strip()

        if ls_text:
            return ls_text.split("\n")
        else:
            # Special case because otherwise split on empty string
            # gives you [''] instead of []
            return []

    def setfattr(self, path, key, val, **kwargs):
        """
        Wrap setfattr.

        :param path: relative to mount point
        :param key: xattr name
        :param val: xattr value
        :return: None
        """
        kwargs['args'] = ["setfattr", "-n", str(key), "-v", str(val), path]
        if kwargs.pop('sudo', False):
            kwargs['args'].insert(0, 'sudo')
            kwargs['omit_sudo'] = False
        return self.run_shell(**kwargs)

    def getfattr(self, path, attr, **kwargs):
        """
        Wrap getfattr: return the values of a named xattr on one file, or
        None if the attribute is not found.

        :return: a string
        """
        kwargs['args'] = ["getfattr", "--only-values", "-n", attr, path]
        if kwargs.pop('sudo', False):
            kwargs['args'].insert(0, 'sudo')
            kwargs['omit_sudo'] = False
        kwargs['wait'] = False
        p = self.run_shell(**kwargs)
        try:
            p.wait()
        except CommandFailedError as e:
            if e.exitstatus == 1 and "No such attribute" in p.stderr.getvalue():
                return None
            else:
                raise

        return str(p.stdout.getvalue())

    def df(self):
        """
        Wrap df: return a dict of usage fields in bytes
        """

        p = self.run_shell(["df", "-B1", "."])
        lines = p.stdout.getvalue().strip().split("\n")
        fs, total, used, avail = lines[1].split()[:4]
        log.warning(lines)

        return {
            "total": int(total),
            "used": int(used),
            "available": int(avail)
        }

    def dir_checksum(self, path=None, follow_symlinks=False):
        cmd = ["find"]
        if follow_symlinks:
            cmd.append("-L")
        if path:
            cmd.append(path)
        cmd.extend(["-type", "f", "-exec", "md5sum", "{}", "+"])
        checksum_text = self.run_shell(cmd).stdout.getvalue().strip()
        checksum_sorted = sorted(checksum_text.split('\n'), key=lambda v: v.split()[1])
        return hashlib.md5(('\n'.join(checksum_sorted)).encode('utf-8')).hexdigest()

    def validate_subvol_options(self):
        mount_subvol_num = self.client_config.get('mount_subvol_num', None)
        if self.cephfs_mntpt and mount_subvol_num is not None:
            log.warning("You cannot specify both: cephfs_mntpt and mount_subvol_num")
            log.info(f"Mounting subvol {mount_subvol_num} for now")

        if mount_subvol_num is not None:
            # mount_subvol must be an index into the subvol path array for the fs
            if not self.cephfs_name:
                self.cephfs_name = 'cephfs'
            assert(hasattr(self.ctx, "created_subvols"))
            # mount_subvol must be specified under client.[0-9] yaml section
            subvol_paths = self.ctx.created_subvols[self.cephfs_name]
            path_to_mount = subvol_paths[mount_subvol_num]
            self.cephfs_mntpt = path_to_mount

CephFSMount = CephFSMountBase
