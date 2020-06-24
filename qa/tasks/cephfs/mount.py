from contextlib import contextmanager
import json
import logging
import datetime
import six
import time
from six import StringIO
from textwrap import dedent
import os
import re
from IPy import IP
from teuthology.orchestra import run
from teuthology.orchestra.run import CommandFailedError, ConnectionLostError
from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)


class CephFSMount(object):
    def __init__(self, ctx, test_dir, client_id, client_remote, brxnet):
        """
        :param test_dir: Global teuthology test dir
        :param client_id: Client ID, the 'foo' in client.foo
        :param client_remote: Remote instance for the host where client will run
        """

        self.ctx = ctx
        self.test_dir = test_dir
        self.client_id = client_id
        self.client_remote = client_remote
        self.mountpoint_dir_name = 'mnt.{id}'.format(id=self.client_id)
        self._mountpoint = None
        self.fs = None
        self.mounted = False
        self._netns_name = None
        self.nsid = -1
        if brxnet is None:
            self.ceph_brx_net = '192.168.0.0/16'
        else:
            self.ceph_brx_net = brxnet

        self.test_files = ['a', 'b', 'c']

        self.background_procs = []

    def _parse_netns_name(self):
        self._netns_name = '-'.join(["ceph-ns",
                                     re.sub(r'/+', "-", self.mountpoint)])

    @property
    def mountpoint(self):
        if self._mountpoint == None:
            self._mountpoint= os.path.join(
                self.test_dir, '{dir_name}'.format(dir_name=self.mountpoint_dir_name))
        return self._mountpoint

    @mountpoint.setter
    def mountpoint(self, path):
        if not isinstance(path, str):
            raise RuntimeError('path should be of str type.')
        self._mountpoint = path

    @property
    def netns_name(self):
        if self._netns_name == None:
            self._parse_netns_name()
        return self._netns_name

    @netns_name.setter
    def netns_name(self, name):
        self._netns_name = name

    def is_mounted(self):
        return self.mounted

    def setupfs(self, name=None):
        if name is None and self.fs is not None:
            # Previous mount existed, reuse the old name
            name = self.fs.name
        self.fs = Filesystem(self.ctx, name=name)
        log.info('Wait for MDS to reach steady state...')
        self.fs.wait_for_daemons()
        log.info('Ready to start {}...'.format(type(self).__name__))

    def _bringup_network_manager_service(self):
        args = ["sudo", "bash", "-c",
                "systemctl start NetworkManager"]
        self.client_remote.run(args=args, timeout=(5*60))

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

            args = ["sudo", "bash", "-c", "ip link add name ceph-brx type bridge"]
            self.client_remote.run(args=args, timeout=(5*60))
            args = ["sudo", "bash", "-c", "ip link set ceph-brx up"]
            self.client_remote.run(args=args, timeout=(5*60))
            args = ["sudo", "bash", "-c",
                    "ip addr add {0}/{1} brd {2} dev ceph-brx".format(ip, mask, brd)]
            self.client_remote.run(args=args, timeout=(5*60))
        
        args = "echo 1 | sudo tee /proc/sys/net/ipv4/ip_forward"
        self.client_remote.run(args=args, timeout=(5*60))
        
        # Setup the NAT
        p = self.client_remote.run(args=['route'], stderr=StringIO(),
                                   stdout=StringIO(), timeout=(5*60))
        p = re.findall(r'default .*', p.stdout.getvalue())
        if p == False:
            raise RuntimeError("No default gw found")
        gw = p[0].split()[7]
        args = ["sudo", "bash", "-c",
                "iptables -A FORWARD -o {0} -i ceph-brx -j ACCEPT".format(gw)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "iptables -A FORWARD -i {0} -o ceph-brx -j ACCEPT".format(gw)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "iptables -t nat -A POSTROUTING -s {0}/{1} -o {2} -j MASQUERADE".format(ip, mask, gw)]
        self.client_remote.run(args=args, timeout=(5*60))

    def _setup_netns(self):
        p = self.client_remote.run(args=['ip', 'netns', 'list'],
                                   stderr=StringIO(), stdout=StringIO(),
                                   timeout=(5*60))
        p = p.stdout.getvalue().strip()
        if re.match(self.netns_name, p) is not None:
            raise RuntimeError("the netns '{}' already exists!".format(self.netns_name))

        # Get the netns name list
        netns_list = re.findall(r'[^()\s][-.\w]+[^():\s]', p)

        # Get an uniq netns id
        nsid = 0
        while True:
            p = self.client_remote.run(args=['ip', 'netns', 'list-id'],
                                       stderr=StringIO(), stdout=StringIO(),
                                       timeout=(5*60))
            p = re.search(r"nsid {} ".format(nsid), p.stdout.getvalue())
            if p is None:
                break

            nsid += 1

        self.nsid = nsid;

        # Add one new netns and set it id
        args = ["sudo", "bash", "-c",
                "ip netns add {0}".format(self.netns_name)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip netns set {0} {1}".format(self.netns_name, nsid)]
        self.client_remote.run(args=args, timeout=(5*60))

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
                args = ["sudo", "bash", "-c",
                        "ip netns exec {0} ip addr".format(ns_name)]
                p = self.client_remote.run(args=args, stderr=StringIO(),
                                           stdout=StringIO(), timeout=(5*60))
                q = re.search("{0}".format(ip), p.stdout.getvalue())
                if q is not None:
                    found = True
                    break

            if found == False:
                break

        mask = self.ceph_brx_net.split('/')[1]
        brd = IP(self.ceph_brx_net).broadcast()

        log.info("Setuping the netns '{0}' with {1}/{2}".format(self.netns_name, ip, mask))

        # Setup the veth interfaces
        args = ["sudo", "bash", "-c",
                "ip link add veth0 netns {0} type veth peer name brx.{1}".format(self.netns_name, nsid)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip netns exec {0} ip addr add {1}/{2} brd {3} dev veth0".format(self.netns_name, ip, mask, brd)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip netns exec {0} ip link set veth0 up".format(self.netns_name)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip netns exec {0} ip link set lo up".format(self.netns_name)]
        self.client_remote.run(args=args, timeout=(5*60))

        brxip = IP(self.ceph_brx_net)[-2]
        args = ["sudo", "bash", "-c",
                "ip netns exec {0} ip route add default via {1}".format(self.netns_name, brxip)]
        self.client_remote.run(args=args, timeout=(5*60))

        # Bring up the brx interface and join it to 'ceph-brx'
        args = ["sudo", "bash", "-c",
                "ip link set brx.{0} up".format(nsid)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip link set dev brx.{0} master ceph-brx".format(nsid)]
        self.client_remote.run(args=args, timeout=(5*60))

    def _cleanup_netns(self):
        if self.nsid == -1:
            return
        log.info("Removing the netns '{0}'".format(self.netns_name))

        # Delete the netns and the peer veth interface
        args = ["sudo", "bash", "-c",
                "ip link set brx.{0} down".format(self.nsid)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip link delete brx.{0}".format(self.nsid)]
        self.client_remote.run(args=args, timeout=(5*60))

        args = ["sudo", "bash", "-c",
                "ip netns delete {0}".format(self.netns_name)]
        self.client_remote.run(args=args, timeout=(5*60))

        self.nsid = -1

    def _cleanup_brx_and_nat(self):
        brx = self.client_remote.run(args=['ip', 'addr'], stderr=StringIO(),
                                     stdout=StringIO(), timeout=(5*60))
        brx = re.findall(r'inet .* ceph-brx', brx.stdout.getvalue())
        if not brx:
            return

        # If we are the last netns, will delete the ceph-brx
        args = ["sudo", "bash", "-c", "ip link show"]
        p = self.client_remote.run(args=args, stdout=StringIO(),
                                   timeout=(5*60))
        _list = re.findall(r'brx\.', p.stdout.getvalue().strip())
        if len(_list) != 0:
            return

        log.info("Removing the 'ceph-brx'")

        args = ["sudo", "bash", "-c",
                "ip link set ceph-brx down"]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "ip link delete ceph-brx"]
        self.client_remote.run(args=args, timeout=(5*60))

        # Drop the iptables NAT rules
        ip = IP(self.ceph_brx_net)[-2]
        mask = self.ceph_brx_net.split('/')[1]

        p = self.client_remote.run(args=['route'], stderr=StringIO(),
                                   stdout=StringIO(), timeout=(5*60))
        p = re.findall(r'default .*', p.stdout.getvalue())
        if p == False:
            raise RuntimeError("No default gw found")
        gw = p[0].split()[7]
        args = ["sudo", "bash", "-c",
                "iptables -D FORWARD -o {0} -i ceph-brx -j ACCEPT".format(gw)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "iptables -D FORWARD -i {0} -o ceph-brx -j ACCEPT".format(gw)]
        self.client_remote.run(args=args, timeout=(5*60))
        args = ["sudo", "bash", "-c",
                "iptables -t nat -D POSTROUTING -s {0}/{1} -o {2} -j MASQUERADE".format(ip, mask, gw)]
        self.client_remote.run(args=args, timeout=(5*60))

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
        log.info("Cleaning the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))
        self._cleanup_netns()
        self._cleanup_brx_and_nat()

    def suspend_netns(self):
        """
        Suspend the netns veth interface.
        """
        if self.nsid == -1:
            return

        log.info("Suspending the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))

        args = ["sudo", "bash", "-c",
                "ip link set brx.{0} down".format(self.nsid)]
        self.client_remote.run(args=args, timeout=(5*60))

    def resume_netns(self):
        """
        Resume the netns veth interface.
        """
        if self.nsid == -1:
            return

        log.info("Resuming the '{0}' netns for '{1}'".format(self._netns_name, self.mountpoint))

        args = ["sudo", "bash", "-c",
                "ip link set brx.{0} up".format(self.nsid)]
        self.client_remote.run(args=args, timeout=(5*60))

    def mount(self, mount_path=None, mount_fs_name=None, mountpoint=None, mount_options=[]):
        raise NotImplementedError()

    def mount_wait(self, mount_path=None, mount_fs_name=None, mountpoint=None, mount_options=[]):
        self.mount(mount_path=mount_path, mount_fs_name=mount_fs_name, mountpoint=mountpoint,
                   mount_options=mount_options)
        self.wait_until_mounted()

    def umount(self):
        raise NotImplementedError()

    def umount_wait(self, force=False, require_clean=False):
        """

        :param force: Expect that the mount will not shutdown cleanly: kill
                      it hard.
        :param require_clean: Wait for the Ceph client associated with the
                              mount (e.g. ceph-fuse) to terminate, and
                              raise if it doesn't do so cleanly.
        :return:
        """
        raise NotImplementedError()

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
        stderr = StringIO()
        try:
            self.client_remote.run(
                args=[
                    'rmdir',
                    '--',
                    self.mountpoint,
                ],
                cwd=self.test_dir,
                stderr=stderr,
                timeout=(60*5),
                check_status=False,
            )
        except CommandFailedError:
            if "No such file or directory" in stderr.getvalue():
                pass
            else:
                raise

        self.cleanup_netns()

    def wait_until_mounted(self):
        raise NotImplementedError()

    def get_keyring_path(self):
        return '/etc/ceph/ceph.client.{id}.keyring'.format(id=self.client_id)

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

    def is_blacklisted(self):
        addr = self.get_global_addr()
        blacklist = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "blacklist", "ls", "--format=json"))
        for b in blacklist:
            if addr == b["addr"]:
                return True
        return False

    def create_file(self, filename='testfile', dirname=None, user=None,
                    check_status=True):
        assert(self.is_mounted())

        if not os.path.isabs(filename):
            if dirname:
                if os.path.isabs(dirname):
                    path = os.path.join(dirname, filename)
                else:
                    path = os.path.join(self.mountpoint, dirname, filename)
            else:
                path = os.path.join(self.mountpoint, filename)
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
                'sudo', 'touch', os.path.join(self.mountpoint, suffix)
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
                'sudo', 'ls', os.path.join(self.mountpoint, suffix)
            ], check_status=False)
            if r.exitstatus != 0:
                raise RuntimeError("Expected file {0} not found".format(suffix))

    def create_destroy(self):
        assert(self.is_mounted())

        filename = "{0} {1}".format(datetime.datetime.now(), self.client_id)
        log.debug("Creating test file {0}".format(filename))
        self.client_remote.run(args=[
            'sudo', 'touch', os.path.join(self.mountpoint, filename)
        ])
        log.debug("Deleting test file {0}".format(filename))
        self.client_remote.run(args=[
            'sudo', 'rm', '-f', os.path.join(self.mountpoint, filename)
        ])

    def _run_python(self, pyscript, py_version='python3'):
        return self.client_remote.run(
               args=['sudo', 'adjust-ulimits', 'daemon-helper', 'kill',
                     py_version, '-c', pyscript], wait=False, stdin=run.PIPE,
               stdout=StringIO())

    def run_python(self, pyscript, py_version='python3'):
        p = self._run_python(pyscript, py_version)
        p.wait()
        return six.ensure_str(p.stdout.getvalue().strip())

    def run_shell(self, args, wait=True, stdin=None, check_status=True,
                  cwd=None, omit_sudo=True):
        args = args.split() if isinstance(args, str) else args
        # XXX: all commands ran with CephFS mount as CWD must be executed with
        #  superuser privileges when tests are being run using teuthology.
        if args[0] != 'sudo':
            args.insert(0, 'sudo')
        if not cwd:
            cwd = self.mountpoint

        return self.client_remote.run(args=args, stdin=stdin, wait=wait,
                                      stdout=StringIO(), stderr=StringIO(),
                                      cwd=cwd, check_status=check_status)

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
        return self.run_shell(**kwargs)

    def run_as_root(self, **kwargs):
        """
        Accepts same arguments as run_shell().
        """
        kwargs['user'] = 'root'
        return self.run_as_user(**kwargs)

    def _verify(self, proc, retval=None, errmsg=None):
        if retval:
            msg = ('expected return value: {}\nreceived return value: '
                   '{}\n'.format(retval, proc.returncode))
            assert proc.returncode == retval, msg

        if errmsg:
            stderr = proc.stderr.getvalue().lower()
            msg = ('didn\'t find given string in stderr -\nexpected string: '
                   '{}\nreceived error message: {}\nnote: received error '
                   'message is converted to lowercase'.format(errmsg, stderr))
            assert errmsg in stderr, msg

    def negtestcmd(self, args, retval=None, errmsg=None, stdin=None,
                   cwd=None, wait=True):
        """
        Conduct a negative test for the given command.

        retval and errmsg are parameters to confirm the cause of command
        failure.
        """
        proc = self.run_shell(args=args, wait=wait, stdin=stdin, cwd=cwd,
                              check_status=False)
        self._verify(proc, retval, errmsg)
        return proc

    def negtestcmd_as_user(self, args, user, retval=None, errmsg=None,
                           stdin=None, cwd=None, wait=True):
        proc = self.run_as_user(args=args, user=user, wait=wait, stdin=stdin,
                                cwd=cwd, check_status=False)
        self._verify(proc, retval, errmsg)
        return proc

    def negtestcmd_as_root(self, args, retval=None, errmsg=None, stdin=None,
                           cwd=None, wait=True):
        proc = self.run_as_root(args=args, wait=wait, stdin=stdin, cwd=cwd,
                                check_status=False)
        self._verify(proc, retval, errmsg)
        return proc

    def open_no_data(self, basename):
        """
        A pure metadata operation
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        p = self._run_python(dedent(
            """
            f = open("{path}", 'w')
            """.format(path=path)
        ))
        p.wait()

    def open_background(self, basename="background_file", write=True):
        """
        Open a file for writing, then block such that the client
        will hold a capability.

        Don't return until the remote process has got as far as opening
        the file, then return the RemoteProcess instance.
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        if write:
            pyscript = dedent("""
                import time

                with open("{path}", 'w') as f:
                    f.write('content')
                    f.flush()
                    f.write('content2')
                    while True:
                        time.sleep(1)
                """).format(path=path)
        else:
            pyscript = dedent("""
                import time

                with open("{path}", 'r') as f:
                    while True:
                        time.sleep(1)
                """).format(path=path)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)

        # This wait would not be sufficient if the file had already
        # existed, but it's simple and in practice users of open_background
        # are not using it on existing files.
        self.wait_for_visible(basename)

        return rproc

    def wait_for_dir_empty(self, dirname, timeout=30):
        i = 0
        dirpath = os.path.join(self.mountpoint, dirname)
        while i < timeout:
            nr_entries = int(self.getfattr(dirpath, "ceph.dir.entries"))
            if nr_entries == 0:
                log.debug("Directory {0} seen empty from {1} after {2}s ".format(
                    dirname, self.client_id, i))
                return
            else:
                time.sleep(1)
                i += 1

        raise RuntimeError("Timed out after {0}s waiting for {1} to become empty from {2}".format(
            i, dirname, self.client_id))

    def wait_for_visible(self, basename="background_file", timeout=30):
        i = 0
        while i < timeout:
            r = self.client_remote.run(args=[
                'sudo', 'ls', os.path.join(self.mountpoint, basename)
            ], check_status=False)
            if r.exitstatus == 0:
                log.debug("File {0} became visible from {1} after {2}s".format(
                    basename, self.client_id, i))
                return
            else:
                time.sleep(1)
                i += 1

        raise RuntimeError("Timed out after {0}s waiting for {1} to become visible from {2}".format(
            i, basename, self.client_id))

    def lock_background(self, basename="background_file", do_flock=True):
        """
        Open and lock a files for writing, hold the lock in a background process
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        script_builder = """
            import time
            import fcntl
            import struct"""
        if do_flock:
            script_builder += """
            f1 = open("{path}-1", 'w')
            fcntl.flock(f1, fcntl.LOCK_EX | fcntl.LOCK_NB)"""
        script_builder += """
            f2 = open("{path}-2", 'w')
            lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
            fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            while True:
                time.sleep(1)
            """

        pyscript = dedent(script_builder).format(path=path)

        log.info("lock_background file {0}".format(basename))
        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def lock_and_release(self, basename="background_file"):
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        script = """
            import time
            import fcntl
            import struct
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

        path = os.path.join(self.mountpoint, basename)

        script_builder = """
            import fcntl
            import errno
            import struct"""
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
            'sudo', 'python3', '-c', pyscript
        ])

    def write_background(self, basename="background_file", loop=False):
        """
        Open a file for writing, complete as soon as you can
        :param basename:
        :return:
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        pyscript = dedent("""
            import os
            import time

            fd = os.open("{path}", os.O_RDWR | os.O_CREAT, 0o644)
            try:
                while True:
                    os.write(fd, b'content')
                    time.sleep(1)
                    if not {loop}:
                        break
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
            path=os.path.join(self.mountpoint, filename),
            size=size
        )))

    def validate_test_pattern(self, filename, size):
        log.info("Validating {0} bytes from {1}".format(size, filename))
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
            path=os.path.join(self.mountpoint, filename),
            size=size
        )))

    def open_n_background(self, fs_path, count):
        """
        Open N files for writing, hold them open in a background process

        :param fs_path: Path relative to CephFS root, e.g. "foo/bar"
        :return: a RemoteProcess
        """
        assert(self.is_mounted())

        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import sys
            import time
            import os

            n = {count}
            abs_path = "{abs_path}"

            if not os.path.exists(os.path.dirname(abs_path)):
                os.makedirs(os.path.dirname(abs_path))

            handles = []
            for i in range(0, n):
                fname = "{{0}}_{{1}}".format(abs_path, i)
                handles.append(open(fname, 'w'))

            while True:
                time.sleep(1)
            """).format(abs_path=abs_path, count=count)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def create_n_files(self, fs_path, count, sync=False):
        assert(self.is_mounted())

        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import sys
            import time
            import os

            n = {count}
            abs_path = "{abs_path}"

            if not os.path.exists(os.path.dirname(abs_path)):
                os.makedirs(os.path.dirname(abs_path))

            for i in range(0, n):
                fname = "{{0}}_{{1}}".format(abs_path, i)
                with open(fname, 'w') as f:
                    f.write('content')
                    if {sync}:
                        f.flush()
                        os.fsync(f.fileno())
            """).format(abs_path=abs_path, count=count, sync=str(sync))

        self.run_python(pyscript)

    def teardown(self):
        for p in self.background_procs:
            log.info("Terminating background process")
            self._kill_background(p)

        self.background_procs = []

    def _kill_background(self, p):
        if p.stdin:
            p.stdin.close()
            try:
                p.wait()
            except (CommandFailedError, ConnectionLostError):
                pass

    def kill_background(self, p):
        """
        For a process that was returned by one of the _background member functions,
        kill it hard.
        """
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

    def lstat(self, fs_path, follow_symlinks=False, wait=True):
        return self.stat(fs_path, follow_symlinks=False, wait=True)

    def stat(self, fs_path, follow_symlinks=True, wait=True):
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
        abs_path = os.path.join(self.mountpoint, fs_path)
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
        proc = self._run_python(pyscript)
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
        abs_path = os.path.join(self.mountpoint, fs_path)
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
        abs_path = os.path.join(self.mountpoint, fs_path)

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
        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import os
            import stat

            print(os.stat("{path}").st_nlink)
            """).format(path=abs_path)

        proc = self._run_python(pyscript)
        proc.wait()
        return int(proc.stdout.getvalue().strip())

    def ls(self, path=None):
        """
        Wrap ls: return a list of strings
        """
        cmd = ["ls"]
        if path:
            cmd.append(path)

        ls_text = self.run_shell(cmd).stdout.getvalue().strip()

        if ls_text:
            return ls_text.split("\n")
        else:
            # Special case because otherwise split on empty string
            # gives you [''] instead of []
            return []

    def setfattr(self, path, key, val):
        """
        Wrap setfattr.

        :param path: relative to mount point
        :param key: xattr name
        :param val: xattr value
        :return: None
        """
        self.run_shell(["setfattr", "-n", key, "-v", val, path])

    def getfattr(self, path, attr):
        """
        Wrap getfattr: return the values of a named xattr on one file, or
        None if the attribute is not found.

        :return: a string
        """
        p = self.run_shell(["getfattr", "--only-values", "-n", attr, path], wait=False)
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
