"""
Useful hack: override Filesystem and Mount interfaces to run a CephFSTestCase against a vstart
ceph instance instead of a packaged/installed cluster.  Use this to turn around test cases
quickly during development.

For example, if you have teuthology, ceph-qa-suite and ceph all in ~git, then you would:

    # Activate the teuthology virtualenv
    source ~/git/teuthology/virtualenv/bin/activate
    # Go into your ceph source tree
    cd ~/git/ceph/src
    # Start a vstart cluster
    MDS=2 MON=1 OSD=3 ./vstart.sh -n
    # Invoke a test using this script, with PYTHONPATH set appropriately
    PYTHONPATH=~/git/teuthology/:~/git/ceph-qa-suite/ python ~/git/ceph-qa-suite/tasks/cephfs/vstart_runner.py

If you built out of tree with CMake, then switch to your build directory before executing vstart_runner.

"""

from StringIO import StringIO
from collections import defaultdict
import getpass
import signal
import tempfile
import threading
import datetime
import shutil
import re
import os
import time
import json
import sys
import errno
from unittest import suite
import unittest
from teuthology.orchestra.run import Raw, quote
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.config import config as teuth_config

import logging

log = logging.getLogger(__name__)

handler = logging.FileHandler("./vstart_runner.log")
formatter = logging.Formatter(
    fmt=u'%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)

try:
    from teuthology.exceptions import CommandFailedError
    from tasks.ceph_manager import CephManager
    from tasks.cephfs.fuse_mount import FuseMount
    from tasks.cephfs.filesystem import Filesystem
    from teuthology.contextutil import MaxWhileTries
    from teuthology.task import interactive
except ImportError:
    sys.stderr.write("***\nError importing packages, have you activated your teuthology virtualenv "
                     "and set PYTHONPATH to point to teuthology and ceph-qa-suite?\n***\n\n")
    raise

# Must import after teuthology because of gevent monkey patching
import subprocess

if os.path.exists("./CMakeCache.txt"):
    # Running in build dir of a cmake build
    BIN_PREFIX = "./src/"
else:
    # Running in src/ of an autotools build
    BIN_PREFIX = "./"


class LocalRemoteProcess(object):
    def __init__(self, args, subproc, check_status, stdout, stderr):
        self.args = args
        self.subproc = subproc
        if stdout is None:
            self.stdout = StringIO()
        else:
            self.stdout = stdout

        if stderr is None:
            self.stderr = StringIO()
        else:
            self.stderr = stderr

        self.check_status = check_status
        self.exitstatus = self.returncode = None

    def wait(self):
        if self.finished:
            # Avoid calling communicate() on a dead process because it'll
            # give you stick about std* already being closed
            if self.exitstatus != 0:
                raise CommandFailedError(self.args, self.exitstatus)
            else:
                return

        out, err = self.subproc.communicate()
        self.stdout.write(out)
        self.stderr.write(err)

        self.exitstatus = self.returncode = self.subproc.returncode

        if self.exitstatus != 0:
            sys.stderr.write(out)
            sys.stderr.write(err)

        if self.check_status and self.exitstatus != 0:
            raise CommandFailedError(self.args, self.exitstatus)

    @property
    def finished(self):
        if self.exitstatus is not None:
            return True

        if self.subproc.poll() is not None:
            out, err = self.subproc.communicate()
            self.stdout.write(out)
            self.stderr.write(err)
            self.exitstatus = self.returncode = self.subproc.returncode
            return True
        else:
            return False

    def kill(self):
        log.info("kill ")
        if self.subproc.pid and not self.finished:
            log.info("kill: killing pid {0} ({1})".format(
                self.subproc.pid, self.args))
            safe_kill(self.subproc.pid)
        else:
            log.info("kill: already terminated ({0})".format(self.args))

    @property
    def stdin(self):
        class FakeStdIn(object):
            def __init__(self, mount_daemon):
                self.mount_daemon = mount_daemon

            def close(self):
                self.mount_daemon.kill()

        return FakeStdIn(self)


class LocalRemote(object):
    """
    Amusingly named class to present the teuthology RemoteProcess interface when we are really
    running things locally for vstart

    Run this inside your src/ dir!
    """

    def __init__(self):
        self.name = "local"
        self.hostname = "localhost"
        self.user = getpass.getuser()

    def get_file(self, path, sudo, dest_dir):
        tmpfile = tempfile.NamedTemporaryFile(delete=False).name
        shutil.copy(path, tmpfile)
        return tmpfile

    def put_file(self, src, dst, sudo=False):
        shutil.copy(src, dst)

    def run(self, args, check_status=True, wait=True,
            stdout=None, stderr=None, cwd=None, stdin=None,
            logger=None, label=None):
        log.info("run args={0}".format(args))

        # We don't need no stinkin' sudo
        args = [a for a in args if a != "sudo"]

        # We have to use shell=True if any run.Raw was present, e.g. &&
        shell = any([a for a in args if isinstance(a, Raw)])

        if shell:
            filtered = []
            i = 0
            while i < len(args):
                if args[i] == 'adjust-ulimits':
                    i += 1
                elif args[i] == 'ceph-coverage':
                    i += 2
                elif args[i] == 'timeout':
                    i += 2
                else:
                    filtered.append(args[i])
                    i += 1

            args = quote(filtered)
            log.info("Running {0}".format(args))

            subproc = subprocess.Popen(args,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       stdin=subprocess.PIPE,
                                       cwd=cwd,
                                       shell=True)
        else:
            log.info("Running {0}".format(args))

            for arg in args:
                if not isinstance(arg, basestring):
                    raise RuntimeError("Oops, can't handle arg {0} type {1}".format(
                        arg, arg.__class__
                    ))

            subproc = subprocess.Popen(args,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       stdin=subprocess.PIPE,
                                       cwd=cwd)

        if stdin:
            if not isinstance(stdin, basestring):
                raise RuntimeError("Can't handle non-string stdins on a vstart cluster")

            # Hack: writing to stdin is not deadlock-safe, but it "always" works
            # as long as the input buffer is "small"
            subproc.stdin.write(stdin)

        proc = LocalRemoteProcess(
            args, subproc, check_status,
            stdout, stderr
        )

        if wait:
            proc.wait()

        return proc


# FIXME: twiddling vstart daemons is likely to be unreliable, we should probably just let vstart
# run RADOS and run the MDS daemons directly from the test runner
class LocalDaemon(object):
    def __init__(self, daemon_type, daemon_id):
        self.daemon_type = daemon_type
        self.daemon_id = daemon_id
        self.controller = LocalRemote()
        self.proc = None

    @property
    def remote(self):
        return LocalRemote()

    def running(self):
        return self._get_pid() is not None

    def _get_pid(self):
        """
        Return PID as an integer or None if not found
        """
        ps_txt = self.controller.run(
            args=["ps", "aux"]
        ).stdout.getvalue().strip()
        lines = ps_txt.split("\n")[1:]

        for line in lines:
            if line.find("ceph-{0} -i {1}".format(self.daemon_type, self.daemon_id)) != -1:
                log.info("Found ps line for daemon: {0}".format(line))
                return int(line.split()[1])

        return None

    def wait(self, timeout):
        waited = 0
        while self._get_pid() is not None:
            if waited > timeout:
                raise MaxWhileTries("Timed out waiting for daemon {0}.{1}".format(self.daemon_type, self.daemon_id))
            time.sleep(1)
            waited += 1

    def stop(self, timeout=300):
        if not self.running():
            log.error('tried to stop a non-running daemon')
            return

        pid = self._get_pid()
        log.info("Killing PID {0} for {1}.{2}".format(pid, self.daemon_type, self.daemon_id))
        os.kill(pid, signal.SIGKILL)
        self.wait(timeout=timeout)

    def restart(self):
        if self._get_pid() is not None:
            self.stop()

        self.proc = self.controller.run([os.path.join(BIN_PREFIX, "./ceph-{0}".format(self.daemon_type)), "-i", self.daemon_id])


def safe_kill(pid):
    """
    os.kill annoyingly raises exception if process already dead.  Ignore it.
    """
    try:
        return os.kill(pid, signal.SIGKILL)
    except OSError as e:
        if e.errno == errno.ESRCH:
            # Raced with process termination
            pass
        else:
            raise


class LocalFuseMount(FuseMount):
    def __init__(self, test_dir, client_id):
        super(LocalFuseMount, self).__init__(None, test_dir, client_id, LocalRemote())

    @property
    def config_path(self):
        return "./ceph.conf"

    def get_keyring_path(self):
        # This is going to end up in a config file, so use an absolute path
        # to avoid assumptions about daemons' pwd
        return os.path.abspath("./client.{0}.keyring".format(self.client_id))

    def run_shell(self, args, wait=True):
        # FIXME maybe should add a pwd arg to teuthology.orchestra so that
        # the "cd foo && bar" shenanigans isn't needed to begin with and
        # then we wouldn't have to special case this
        return self.client_remote.run(
            args, wait=wait, cwd=self.mountpoint
        )

    @property
    def _prefix(self):
        # FuseMount only uses the prefix for running ceph, which in cmake or autotools is in
        # the present path
        return "./"

    def _asok_path(self):
        # In teuthology, the asok is named after the PID of the ceph-fuse process, because it's
        # run foreground.  When running it daemonized however, the asok is named after
        # the PID of the launching process, not the long running ceph-fuse process.  Therefore
        # we need to give an exact path here as the logic for checking /proc/ for which
        # asok is alive does not work.
        path = "./out/client.{0}.{1}.asok".format(self.client_id, self.fuse_daemon.subproc.pid)
        log.info("I think my launching pid was {0}".format(self.fuse_daemon.subproc.pid))
        return path

    def umount(self):
        if self.is_mounted():
            super(LocalFuseMount, self).umount()

    def mount(self, mount_path=None):
        self.client_remote.run(
            args=[
                'mkdir',
                '--',
                self.mountpoint,
            ],
        )

        def list_connections():
            self.client_remote.run(
                args=["mount", "-t", "fusectl", "/sys/fs/fuse/connections", "/sys/fs/fuse/connections"],
                check_status=False
            )
            p = self.client_remote.run(
                args=["ls", "/sys/fs/fuse/connections"],
                check_status=False
            )
            if p.exitstatus != 0:
                log.warn("ls conns failed with {0}, assuming none".format(p.exitstatus))
                return []

            ls_str = p.stdout.getvalue().strip()
            if ls_str:
                return [int(n) for n in ls_str.split("\n")]
            else:
                return []

        # Before starting ceph-fuse process, note the contents of
        # /sys/fs/fuse/connections
        pre_mount_conns = list_connections()
        log.info("Pre-mount connections: {0}".format(pre_mount_conns))

        prefix = [os.path.join(BIN_PREFIX, "ceph-fuse")]
        if os.getuid() != 0:
            prefix += ["--client-die-on-failed-remount=false"]

        if mount_path is not None:
            prefix += ["--client_mountpoint={0}".format(mount_path)]

        self.fuse_daemon = self.client_remote.run(args=
                                            prefix + [
                                                "-f",
                                                "--name",
                                                "client.{0}".format(self.client_id),
                                                self.mountpoint
                                            ], wait=False)

        log.info("Mounted client.{0} with pid {1}".format(self.client_id, self.fuse_daemon.subproc.pid))

        # Wait for the connection reference to appear in /sys
        waited = 0
        post_mount_conns = list_connections()
        while len(post_mount_conns) <= len(pre_mount_conns):
            if self.fuse_daemon.finished:
                # Did mount fail?  Raise the CommandFailedError instead of
                # hitting the "failed to populate /sys/" timeout
                self.fuse_daemon.wait()
            time.sleep(1)
            waited += 1
            if waited > 30:
                raise RuntimeError("Fuse mount failed to populate /sys/ after {0} seconds".format(
                    waited
                ))
            post_mount_conns = list_connections()

        log.info("Post-mount connections: {0}".format(post_mount_conns))

        # Record our fuse connection number so that we can use it when
        # forcing an unmount
        new_conns = list(set(post_mount_conns) - set(pre_mount_conns))
        if len(new_conns) == 0:
            raise RuntimeError("New fuse connection directory not found ({0})".format(new_conns))
        elif len(new_conns) > 1:
            raise RuntimeError("Unexpectedly numerous fuse connections {0}".format(new_conns))
        else:
            self._fuse_conn = new_conns[0]

    def _run_python(self, pyscript):
        """
        Override this to remove the daemon-helper prefix that is used otherwise
        to make the process killable.
        """
        return self.client_remote.run(args=[
            'python', '-c', pyscript
        ], wait=False)


class LocalCephManager(CephManager):
    def __init__(self):
        # Deliberately skip parent init, only inheriting from it to get
        # util methods like osd_dump that sit on top of raw_cluster_cmd
        self.controller = LocalRemote()

        # A minority of CephManager fns actually bother locking for when
        # certain teuthology tests want to run tasks in parallel
        self.lock = threading.RLock()

    def find_remote(self, daemon_type, daemon_id):
        """
        daemon_type like 'mds', 'osd'
        daemon_id like 'a', '0'
        """
        return LocalRemote()

    def run_ceph_w(self):
        proc = self.controller.run(["./ceph", "-w"], wait=False, stdout=StringIO())
        return proc

    def raw_cluster_cmd(self, *args):
        """
        args like ["osd", "dump"}
        return stdout string
        """
        proc = self.controller.run(["./ceph"] + list(args))
        return proc.stdout.getvalue()

    def raw_cluster_cmd_result(self, *args):
        """
        like raw_cluster_cmd but don't check status, just return rc
        """
        proc = self.controller.run(["./ceph"] + list(args), check_status=False)
        return proc.exitstatus

    def admin_socket(self, daemon_type, daemon_id, command, check_status=True):
        return self.controller.run(
            args=["./ceph", "daemon", "{0}.{1}".format(daemon_type, daemon_id)] + command, check_status=check_status
        )

    # FIXME: copypasta
    def get_mds_status(self, mds):
        """
        Run cluster commands for the mds in order to get mds information
        """
        out = self.raw_cluster_cmd('mds', 'dump', '--format=json')
        j = json.loads(' '.join(out.splitlines()[1:]))
        # collate; for dup ids, larger gid wins.
        for info in j['info'].itervalues():
            if info['name'] == mds:
                return info
        return None

    # FIXME: copypasta
    def get_mds_status_by_rank(self, rank):
        """
        Run cluster commands for the mds in order to get mds information
        check rank.
        """
        j = self.get_mds_status_all()
        # collate; for dup ids, larger gid wins.
        for info in j['info'].itervalues():
            if info['rank'] == rank:
                return info
        return None

    def get_mds_status_all(self):
        """
        Run cluster command to extract all the mds status.
        """
        out = self.raw_cluster_cmd('mds', 'dump', '--format=json')
        j = json.loads(' '.join(out.splitlines()[1:]))
        return j


class LocalFilesystem(Filesystem):
    def __init__(self, ctx):
        # Deliberately skip calling parent constructor
        self._ctx = ctx

        self.admin_remote = LocalRemote()

        self.mds_ids = ctx.daemons.daemons['mds'].keys()
        if not self.mds_ids:
            raise RuntimeError("No MDSs found in ceph.conf!")

        self.mon_manager = LocalCephManager()

        self.mds_daemons = ctx.daemons.daemons["mds"]

        self.client_remote = LocalRemote()

        self._conf = defaultdict(dict)

    @property
    def _prefix(self):
        return BIN_PREFIX

    def set_clients_block(self, blocked, mds_id=None):
        raise NotImplementedError()

    def get_pgs_per_fs_pool(self):
        # FIXME: assuming there are 3 OSDs
        return 3 * int(self.get_config('mon_pg_warn_min_per_osd'))

    def get_config(self, key, service_type=None):
        if service_type is None:
            service_type = 'mon'

        # FIXME hardcoded vstart service IDs
        service_id = {
            'mon': 'a',
            'mds': 'a',
            'osd': '0'
        }[service_type]

        return self.json_asok(['config', 'get', key], service_type, service_id)[key]

    def _write_conf(self):
        # In teuthology, we have the honour of writing the entire ceph.conf, but
        # in vstart land it has mostly already been written and we need to carefully
        # append to it.
        conf_path = "./ceph.conf"
        banner = "\n#LOCAL_TEST\n"
        existing_str = open(conf_path).read()

        if banner in existing_str:
            existing_str = existing_str[0:existing_str.find(banner)]

        existing_str += banner

        for subsys, kvs in self._conf.items():
            existing_str += "\n[{0}]\n".format(subsys)
            for key, val in kvs.items():
                # Comment out existing instance if it exists
                log.info("Searching for existing instance {0}/{1}".format(
                    key, subsys
                ))
                existing_section = re.search("^\[{0}\]$([\n]|[^\[])+".format(
                    subsys
                ), existing_str, re.MULTILINE)

                if existing_section:
                    section_str = existing_str[existing_section.start():existing_section.end()]
                    existing_val = re.search("^\s*[^#]({0}) =".format(key), section_str, re.MULTILINE)
                    if existing_val:
                        start = existing_section.start() + existing_val.start(1)
                        log.info("Found string to replace at {0}".format(
                            start
                        ))
                        existing_str = existing_str[0:start] + "#" + existing_str[start:]

                existing_str += "{0} = {1}\n".format(key, val)

        open(conf_path, "w").write(existing_str)

    def set_ceph_conf(self, subsys, key, value):
        self._conf[subsys][key] = value
        self._write_conf()

    def clear_ceph_conf(self, subsys, key):
        del self._conf[subsys][key]
        self._write_conf()

    def clear_firewall(self):
        # FIXME: unimplemented
        pass


class InteractiveFailureResult(unittest.TextTestResult):
    """
    Specialization that implements interactive-on-error style
    behavior.
    """
    def addFailure(self, test, err):
        super(InteractiveFailureResult, self).addFailure(test, err)
        log.error(self._exc_info_to_string(err, test))
        log.error("Failure in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=None, config=None)

    def addError(self, test, err):
        super(InteractiveFailureResult, self).addError(test, err)
        log.error(self._exc_info_to_string(err, test))
        log.error("Error in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=None, config=None)


def exec_test():
    # Help developers by stopping up-front if their tree isn't built enough for all the
    # tools that the tests might want to use (add more here if needed)
    require_binaries = ["ceph-dencoder", "cephfs-journal-tool", "cephfs-data-scan",
                        "cephfs-table-tool", "ceph-fuse", "rados"]
    missing_binaries = [b for b in require_binaries if not os.path.exists(os.path.join(BIN_PREFIX, b))]
    if missing_binaries:
        log.error("Some ceph binaries missing, please build them: {0}".format(" ".join(missing_binaries)))
        sys.exit(-1)

    test_dir = tempfile.mkdtemp()

    # Create as many of these as the biggest test requires
    clients = ["0", "1", "2"]

    remote = LocalRemote()

    # Tolerate no MDSs or clients running at start
    ps_txt = remote.run(
        args=["ps", "aux"]
    ).stdout.getvalue().strip()
    lines = ps_txt.split("\n")[1:]

    for line in lines:
        if 'ceph-fuse' in line or 'ceph-mds' in line:
            pid = int(line.split()[1])
            log.warn("Killing stray process {0}".format(line))
            os.kill(pid, signal.SIGKILL)

    class LocalCluster(object):
        def __init__(self, rolename="placeholder"):
            self.remotes = {
                remote: [rolename]
            }

        def only(self, requested):
            return self.__class__(rolename=requested)

    teuth_config['test_path'] = test_dir

    class LocalContext(object):
        def __init__(self):
            self.config = {}
            self.teuthology_config = teuth_config
            self.cluster = LocalCluster()
            self.daemons = DaemonGroup()

            # Shove some LocalDaemons into the ctx.daemons DaemonGroup instance so that any
            # tests that want to look these up via ctx can do so.
            # Inspect ceph.conf to see what roles exist
            for conf_line in open("ceph.conf").readlines():
                for svc_type in ["mon", "osd", "mds"]:
                    if svc_type not in self.daemons.daemons:
                        self.daemons.daemons[svc_type] = {}
                    match = re.match("^\[{0}\.(.+)\]$".format(svc_type), conf_line)
                    if match:
                        svc_id = match.group(1)
                        self.daemons.daemons[svc_type][svc_id] = LocalDaemon(svc_type, svc_id)

        def __del__(self):
            shutil.rmtree(self.teuthology_config['test_path'])

    ctx = LocalContext()

    mounts = []
    for client_id in clients:
        # Populate client keyring (it sucks to use client.admin for test clients
        # because it's awkward to find the logs later)
        client_name = "client.{0}".format(client_id)

        if client_name not in open("./keyring").read():
            p = remote.run(args=["./ceph", "auth", "get-or-create", client_name,
                                 "osd", "allow rw",
                                 "mds", "allow",
                                 "mon", "allow r"])

            open("./keyring", "a").write(p.stdout.getvalue())

        mount = LocalFuseMount(test_dir, client_id)
        mounts.append(mount)
        if mount.is_mounted():
            log.warn("unmounting {0}".format(mount.mountpoint))
            mount.umount_wait()
        else:
            if os.path.exists(mount.mountpoint):
                os.rmdir(mount.mountpoint)
    filesystem = LocalFilesystem(ctx)

    from tasks.cephfs_test_runner import DecoratingLoader

    class LogStream(object):
        def __init__(self):
            self.buffer = ""

        def write(self, data):
            self.buffer += data
            if "\n" in self.buffer:
                lines = self.buffer.split("\n")
                for line in lines[:-1]:
                    pass
                    # sys.stderr.write(line + "\n")
                    log.info(line)
                self.buffer = lines[-1]

        def flush(self):
            pass

    decorating_loader = DecoratingLoader({
        "ctx": ctx,
        "mounts": mounts,
        "fs": filesystem
    })

    # For the benefit of polling tests like test_full -- in teuthology land we set this
    # in a .yaml, here it's just a hardcoded thing for the developer's pleasure.
    remote.run(args=["./ceph", "tell", "osd.*", "injectargs", "--osd-mon-report-interval-max", "5"])
    filesystem.set_ceph_conf("osd", "osd_mon_report_interval_max", "5")

    # Vstart defaults to two segments, which very easily gets a "behind on trimming" health warning
    # from normal IO latency.  Increase it for running teests.
    filesystem.set_ceph_conf("mds", "mds log max segments", "10")

    # Make sure the filesystem created in tests has uid/gid that will let us talk to
    # it after mounting it (without having to  go root).  Set in 'global' not just 'mds'
    # so that cephfs-data-scan will pick it up too.
    filesystem.set_ceph_conf("global", "mds root ino uid", "%s" % os.getuid())
    filesystem.set_ceph_conf("global", "mds root ino gid", "%s" % os.getgid())

    # Monkeypatch get_package_version to avoid having to work out what kind of distro we're on
    def _get_package_version(remote, pkg_name):
        # Used in cephfs tests to find fuse version.  Your development workstation *does* have >=2.9, right?
        return "2.9"

    import teuthology.packaging
    teuthology.packaging.get_package_version = _get_package_version

    def enumerate_methods(s):
        for t in s._tests:
            if isinstance(t, suite.BaseTestSuite):
                for sub in enumerate_methods(t):
                    yield sub
            else:
                yield s, t

    interactive_on_error = False

    args = sys.argv[1:]
    flags = [a for a in args if a.startswith("-")]
    modules = [a for a in args if not a.startswith("-")]
    for f in flags:
        if f == "--interactive":
            interactive_on_error = True
        else:
            log.error("Unknown option '{0}'".format(f))
            sys.exit(-1)

    if modules:
        log.info("Executing modules: {0}".format(modules))
        module_suites = []
        for mod_name in modules:
            # Test names like cephfs.test_auto_repair
            log.info("Loaded: {0}".format(list(module_suites)))
            module_suites.append(decorating_loader.loadTestsFromName(mod_name))
        overall_suite = suite.TestSuite(module_suites)
    else:
        log.info("Excuting all tests")
        overall_suite = decorating_loader.discover(
            os.path.dirname(os.path.abspath(__file__))
        )

    # Filter out tests that don't lend themselves to interactive running,
    victims = []
    for case, method in enumerate_methods(overall_suite):
        fn = getattr(method, method._testMethodName)

        drop_test = False

        if hasattr(fn, 'is_long_running') and getattr(fn, 'is_long_running') is True:
            drop_test = True
            log.warn("Dropping test because long running: ".format(method.id()))

        if getattr(fn, "needs_trimming", False) is True:
            drop_test = (os.getuid() != 0)
            log.warn("Dropping test because client trim unavailable: ".format(method.id()))

        if drop_test:
            # Don't drop the test if it was explicitly requested in arguments
            is_named = False
            for named in modules:
                if named.endswith(method.id()):
                    is_named = True
                    break

            if not is_named:
                victims.append((case, method))

    log.info("Disabling {0} tests because of is_long_running or needs_trimming".format(len(victims)))
    for s, method in victims:
        s._tests.remove(method)

    if interactive_on_error:
        result_class = InteractiveFailureResult
    else:
        result_class = unittest.TextTestResult
    fail_on_skip = False

    class LoggingResult(result_class):
        def startTest(self, test):
            log.info("Starting test: {0}".format(self.getDescription(test)))
            test.started_at = datetime.datetime.utcnow()
            return super(LoggingResult, self).startTest(test)

        def stopTest(self, test):
            log.info("Stopped test: {0} in {1}s".format(
                self.getDescription(test),
                (datetime.datetime.utcnow() - test.started_at).total_seconds()
            ))

        def addSkip(self, test, reason):
            if fail_on_skip:
                # Don't just call addFailure because that requires a traceback
                self.failures.append((test, reason))
            else:
                super(LoggingResult, self).addSkip(test, reason)

    # Execute!
    result = unittest.TextTestRunner(
        stream=LogStream(),
        resultclass=LoggingResult,
        verbosity=2,
        failfast=True).run(overall_suite)

    if not result.wasSuccessful():
        result.printErrors()  # duplicate output at end for convenience

        bad_tests = []
        for test, error in result.errors:
            bad_tests.append(str(test))
        for test, failure in result.failures:
            bad_tests.append(str(test))

        sys.exit(-1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    exec_test()
