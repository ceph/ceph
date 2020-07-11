"""
vstart_runner: override Filesystem and Mount interfaces to run a CephFSTestCase against a vstart
ceph instance instead of a packaged/installed cluster.  Use this to turn around test cases
quickly during development.

Simple usage (assuming teuthology and ceph checked out in ~/git):

    # Activate the teuthology virtualenv
    source ~/git/teuthology/virtualenv/bin/activate
    # Go into your ceph build directory
    cd ~/git/ceph/build
    # Invoke a test using this script
    python ~/git/ceph/qa/tasks/vstart_runner.py --create tasks.cephfs.test_data_scan

Alternative usage:

    # Alternatively, if you use different paths, specify them as follows:
    LD_LIBRARY_PATH=`pwd`/lib PYTHONPATH=~/git/teuthology:~/git/ceph/qa:`pwd`/../src/pybind:`pwd`/lib/cython_modules/lib.3 python ~/git/ceph/qa/tasks/vstart_runner.py

    # If you wish to drop to a python shell on failures, use --interactive:
    python ~/git/ceph/qa/tasks/vstart_runner.py --interactive

    # If you wish to run a named test case, pass it as an argument:
    python ~/git/ceph/qa/tasks/vstart_runner.py tasks.cephfs.test_data_scan

    # Also, you can create the cluster once and then run named test cases against it:
    python ~/git/ceph/qa/tasks/vstart_runner.py --create-cluster-only
    python ~/git/ceph/qa/tasks/vstart_runner.py tasks.mgr.dashboard.test_health
    python ~/git/ceph/qa/tasks/vstart_runner.py tasks.mgr.dashboard.test_rgw

"""

from io import StringIO
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
import sys
import errno
from IPy import IP
from unittest import suite, loader
import unittest
import platform
from teuthology.orchestra.run import Raw, quote
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.orchestra.remote import Remote
from teuthology.config import config as teuth_config
from teuthology.contextutil import safe_while
import logging

def init_log():
    global log
    if log is not None:
        del log
    log = logging.getLogger(__name__)

    global logpath
    logpath = './vstart_runner.log'

    handler = logging.FileHandler(logpath)
    formatter = logging.Formatter(
        fmt=u'%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)

log = None
init_log()


def respawn_in_path(lib_path, python_paths):
    execv_cmd = ['python']
    if platform.system() == "Darwin":
        lib_path_var = "DYLD_LIBRARY_PATH"
    else:
        lib_path_var = "LD_LIBRARY_PATH"

    py_binary = os.environ.get("PYTHON", sys.executable)

    if lib_path_var in os.environ:
        if lib_path not in os.environ[lib_path_var]:
            os.environ[lib_path_var] += ':' + lib_path
            os.execvp(py_binary, execv_cmd + sys.argv)
    else:
        os.environ[lib_path_var] = lib_path
        os.execvp(py_binary, execv_cmd + sys.argv)

    for p in python_paths:
        sys.path.insert(0, p)


# Let's use some sensible defaults
if os.path.exists("./CMakeCache.txt") and os.path.exists("./bin"):

    # A list of candidate paths for each package we need
    guesses = [
        ["~/git/teuthology", "~/scm/teuthology", "~/teuthology"],
        ["lib/cython_modules/lib.3"],
        ["../src/pybind"],
    ]

    python_paths = []

    # Up one level so that "tasks.foo.bar" imports work
    python_paths.append(os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
    ))

    for package_guesses in guesses:
        for g in package_guesses:
            g_exp = os.path.abspath(os.path.expanduser(g))
            if os.path.exists(g_exp):
                python_paths.append(g_exp)

    ld_path = os.path.join(os.getcwd(), "lib/")
    print("Using guessed paths {0} {1}".format(ld_path, python_paths))
    respawn_in_path(ld_path, python_paths)


try:
    from teuthology.exceptions import CommandFailedError
    from tasks.ceph_manager import CephManager
    from tasks.cephfs.fuse_mount import FuseMount
    from tasks.cephfs.kernel_mount import KernelMount
    from tasks.cephfs.filesystem import Filesystem, MDSCluster, CephCluster
    from tasks.mgr.mgr_test_case import MgrCluster
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
    BIN_PREFIX = "./bin/"
    SRC_PREFIX = "../src"
else:
    # Running in src/ of an autotools build
    BIN_PREFIX = "./"
    SRC_PREFIX = "./"


def rm_nonascii_chars(var):
    var = var.replace(b'\xe2\x80\x98', b'\'')
    var = var.replace(b'\xe2\x80\x99', b'\'')
    return var

class LocalRemoteProcess(object):
    def __init__(self, args, subproc, check_status, stdout, stderr):
        self.args = args
        self.subproc = subproc
        self.stdout = stdout
        self.stderr = stderr
        # this variable is meant for instance of this class named fuse_daemon.
        # child process of the command launched with sudo must be killed,
        # since killing parent process alone has no impact on the child
        # process.
        self.fuse_pid = -1

        self.check_status = check_status
        self.exitstatus = self.returncode = None

    def wait(self):
        if self.finished:
            # Avoid calling communicate() on a dead process because it'll
            # give you stick about std* already being closed
            if self.check_status and self.exitstatus != 0:
                raise CommandFailedError(self.args, self.exitstatus)
            else:
                return

        out, err = self.subproc.communicate()
        out, err = rm_nonascii_chars(out), rm_nonascii_chars(err)
        if isinstance(self.stdout, StringIO):
            self.stdout.write(out.decode(errors='ignore'))
        elif self.stdout is None:
            pass
        else:
            self.stdout.write(out)
        if isinstance(self.stderr, StringIO):
            self.stderr.write(err.decode(errors='ignore'))
        elif self.stderr is None:
            pass
        else:
            self.stderr.write(err)

        self.exitstatus = self.returncode = self.subproc.returncode

        if self.exitstatus != 0:
            sys.stderr.write(out.decode())
            sys.stderr.write(err.decode())

        if self.check_status and self.exitstatus != 0:
            raise CommandFailedError(self.args, self.exitstatus)

    @property
    def finished(self):
        if self.exitstatus is not None:
            return True

        if self.subproc.poll() is not None:
            out, err = self.subproc.communicate()
            if isinstance(self.stdout, StringIO):
                self.stdout.write(out.decode(errors='ignore'))
            elif self.stdout is None:
                pass
            else:
                self.stdout.write(out)
            if isinstance(self.stderr, StringIO):
                self.stderr.write(err.decode(errors='ignore'))
            elif self.stderr is None:
                pass
            else:
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
            if self.fuse_pid != -1:
                safe_kill(self.fuse_pid)
            else:
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

    os = Remote.os
    arch = Remote.arch

    def __init__(self):
        self.name = "local"
        self.hostname = "localhost"
        self.user = getpass.getuser()

    def get_file(self, path, sudo, dest_dir):
        tmpfile = tempfile.NamedTemporaryFile(delete=False).name
        shutil.copy(path, tmpfile)
        return tmpfile

    # XXX: This method ignores the error raised when src and dst are
    # holding same path. For teuthology, same path still represents
    # different locations as they lie on different machines.
    def put_file(self, src, dst, sudo=False):
        try:
            shutil.copy(src, dst)
        except shutil.SameFileError:
            pass

    # XXX: accepts only two arugments to maintain compatibility with
    # teuthology's mkdtemp.
    def mkdtemp(self, suffix='', parentdir=None):
        from tempfile import mkdtemp

        # XXX: prefix had to be set without that this method failed against
        # Python2.7 -
        # > /usr/lib64/python2.7/tempfile.py(337)mkdtemp()
        # -> file = _os.path.join(dir, prefix + name + suffix)
        # (Pdb) p prefix
        # None
        return mkdtemp(suffix=suffix, prefix='', dir=parentdir)

    def mktemp(self, suffix=None, parentdir=None):
        """
        Make a remote temporary file

        Returns: the path of the temp file created.
        """
        from tempfile import mktemp
        return mktemp(suffix=suffix, dir=parentdir)

    def _perform_checks_and_return_list_of_args(self, args, omit_sudo):
        # Since Python's shell simulation can only work when commands are
        # provided as a list of argumensts...
        if isinstance(args, str):
            args = args.split()

        # We'll let sudo be a part of command even omit flag says otherwise in
        # cases of commands which can normally be ran only by root.
        try:
            if args[args.index('sudo') + 1] in ['-u', 'passwd', 'chown']:
                omit_sudo = False
        except ValueError:
            pass

        # Quotes wrapping a command argument don't work fine in Python's shell
        # simulation if the arguments contains spaces too. E.g. '"ls"' is OK
        # but "ls /" isn't.
        errmsg = "Don't surround arguments commands by quotes if it " + \
                 "contains spaces.\nargs - %s" % (args)
        for arg in args:
            if isinstance(arg, Raw):
                continue

            if arg and (arg[0] in ['"', "'"] or arg[-1] in ['"', "'"]) and \
               (arg.find(' ') != -1 and 0 < arg.find(' ') < len(arg) - 1):
                raise RuntimeError(errmsg)

        # ['sudo', '-u', 'user', '-s', 'path-to-shell', '-c', 'ls', 'a']
        # and ['sudo', '-u', user, '-s', path_to_shell, '-c', 'ls a'] are
        # treated differently by Python's shell simulation. Only latter has
        # the desired effect.
        errmsg = 'The entire command to executed as other user should be a ' +\
                 'single argument.\nargs - %s' % (args)
        if 'sudo' in args and '-u' in args and '-c' in args and \
           args.count('-c') == 1:
            if args.index('-c') != len(args) - 2 and \
               args[args.index('-c') + 2].find('-') == -1:
                raise RuntimeError(errmsg)

        if omit_sudo:
            args = [a for a in args if a != "sudo"]

        return args

    # Wrapper to keep the interface exactly same as that of
    # teuthology.remote.run.
    def run(self, **kwargs):
        return self._do_run(**kwargs)

    # XXX: omit_sudo is set to True since using sudo can change the ownership
    # of files which becomes problematic for following executions of
    # vstart_runner.py.
    def _do_run(self, args, check_status=True, wait=True, stdout=None,
                stderr=None, cwd=None, stdin=None, logger=None, label=None,
                env=None, timeout=None, omit_sudo=True):
        args = self._perform_checks_and_return_list_of_args(args, omit_sudo)

        # We have to use shell=True if any run.Raw was present, e.g. &&
        shell = any([a for a in args if isinstance(a, Raw)])

        # Filter out helper tools that don't exist in a vstart environment
        args = [a for a in args if a not in ('adjust-ulimits',
                                             'ceph-coverage')]

        # Adjust binary path prefix if given a bare program name
        if not isinstance(args[0], Raw) and "/" not in args[0]:
            # If they asked for a bare binary name, and it exists
            # in our built tree, use the one there.
            local_bin = os.path.join(BIN_PREFIX, args[0])
            if os.path.exists(local_bin):
                args = [local_bin] + args[1:]
            else:
                log.debug("'{0}' is not a binary in the Ceph build dir".format(
                    args[0]
                ))

        log.info("Running {0}".format(args))

        if shell:
            subproc = subprocess.Popen(quote(args),
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       stdin=subprocess.PIPE,
                                       cwd=cwd,
                                       shell=True)
        else:
            # Sanity check that we've got a list of strings
            for arg in args:
                if not isinstance(arg, str):
                    raise RuntimeError("Oops, can't handle arg {0} type {1}".format(
                        arg, arg.__class__
                    ))

            subproc = subprocess.Popen(args,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       stdin=subprocess.PIPE,
                                       cwd=cwd,
                                       env=env)

        if stdin:
            # Hack: writing to stdin is not deadlock-safe, but it "always" works
            # as long as the input buffer is "small"
            if isinstance(stdin, str):
                subproc.stdin.write(stdin.encode())
            else:
                subproc.stdin.write(stdin)

        proc = LocalRemoteProcess(
            args, subproc, check_status,
            stdout, stderr
        )

        if wait:
            proc.wait()

        return proc

    # XXX: for compatibility keep this method same as teuthology.orchestra.remote.sh
    # BytesIO is being used just to keep things identical
    def sh(self, script, **kwargs):
        """
        Shortcut for run method.

        Usage:
            my_name = remote.sh('whoami')
            remote_date = remote.sh('date')
        """
        from io import BytesIO

        if 'stdout' not in kwargs:
            kwargs['stdout'] = BytesIO()
        if 'args' not in kwargs:
            kwargs['args'] = script
        proc = self.run(**kwargs)
        out = proc.stdout.getvalue()
        if isinstance(out, bytes):
            return out.decode()
        else:
            return out

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

    def check_status(self):
        if self.proc:
            return self.proc.poll()

    def _get_pid(self):
        """
        Return PID as an integer or None if not found
        """
        ps_txt = self.controller.run(args=["ps", "ww", "-u"+str(os.getuid())],
                                     stdout=StringIO()).\
            stdout.getvalue().strip()
        lines = ps_txt.split("\n")[1:]

        for line in lines:
            if line.find("ceph-{0} -i {1}".format(self.daemon_type, self.daemon_id)) != -1:
                log.info("Found ps line for daemon: {0}".format(line))
                return int(line.split()[0])
        if opt_log_ps_output:
            log.info("No match for {0} {1}: {2}".format(
                self.daemon_type, self.daemon_id, ps_txt))
        else:
            log.info("No match for {0} {1}".format(self.daemon_type,
                     self.daemon_id))
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
        os.kill(pid, signal.SIGTERM)

        waited = 0
        while pid is not None:
            new_pid = self._get_pid()
            if new_pid is not None and new_pid != pid:
                log.info("Killing new PID {0}".format(new_pid))
                pid = new_pid
                os.kill(pid, signal.SIGTERM)

            if new_pid is None:
                break
            else:
                if waited > timeout:
                    raise MaxWhileTries(
                        "Timed out waiting for daemon {0}.{1}".format(
                            self.daemon_type, self.daemon_id))
                time.sleep(1)
                waited += 1

        self.wait(timeout=timeout)

    def restart(self):
        if self._get_pid() is not None:
            self.stop()

        self.proc = self.controller.run(args=[
            os.path.join(BIN_PREFIX, "./ceph-{0}".format(self.daemon_type)),
            "-i", self.daemon_id])

    def signal(self, sig, silent=False):
        if not self.running():
            raise RuntimeError("Can't send signal to non-running daemon")

        os.kill(self._get_pid(), sig)
        if not silent:
            log.info("Sent signal {0} to {1}.{2}".format(sig, self.daemon_type, self.daemon_id))


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

class LocalKernelMount(KernelMount):
    def __init__(self, ctx, test_dir, client_id, brxnet):
        super(LocalKernelMount, self).__init__(ctx, test_dir, client_id, LocalRemote(), brxnet)

    @property
    def config_path(self):
        return "./ceph.conf"

    def get_keyring_path(self):
        # This is going to end up in a config file, so use an absolute path
        # to avoid assumptions about daemons' pwd
        keyring_path = "./client.{0}.keyring".format(self.client_id)
        try:
            os.stat(keyring_path)
        except OSError:
            return os.path.join(os.getcwd(), 'keyring')
        else:
            return keyring_path

    def setupfs(self, name=None):
        if name is None and self.fs is not None:
            # Previous mount existed, reuse the old name
            name = self.fs.name
        self.fs = LocalFilesystem(self.ctx, name=name)
        log.info('Wait for MDS to reach steady state...')
        self.fs.wait_for_daemons()
        log.info('Ready to start {}...'.format(type(self).__name__))

    @property
    def _prefix(self):
        return BIN_PREFIX

    def _asok_path(self):
        # In teuthology, the asok is named after the PID of the ceph-fuse process, because it's
        # run foreground.  When running it daemonized however, the asok is named after
        # the PID of the launching process, not the long running ceph-fuse process.  Therefore
        # we need to give an exact path here as the logic for checking /proc/ for which
        # asok is alive does not work.

        # Load the asok path from ceph.conf as vstart.sh now puts admin sockets
        # in a tmpdir. All of the paths are the same, so no need to select
        # based off of the service type.
        d = "./out"
        with open(self.config_path) as f:
            for line in f:
                asok_conf = re.search("^\s*admin\s+socket\s*=\s*(.*?)[^/]+$", line)
                if asok_conf:
                    d = asok_conf.groups(1)[0]
                    break
        path = "{0}/client.{1}.*.asok".format(d, self.client_id)
        return path

    def mount(self, mount_path=None, mount_fs_name=None, mount_options=[], **kwargs):
        self.setupfs(name=mount_fs_name)
        if opt_use_ns:
            self.using_namespace = True
            self.setup_netns()
        else:
            self.using_namespace = False

        log.info('Mounting kclient client.{id} at {remote} {mnt}...'.format(
            id=self.client_id, remote=self.client_remote, mnt=self.mountpoint))

        self.client_remote.run(
            args=[
                'mkdir',
                '--',
                self.mountpoint,
            ],
            timeout=(5*60),
        )

        if mount_path is None:
            mount_path = "/"

        opts = 'name={id},norequire_active_mds,conf={conf}'.format(id=self.client_id,
                                                        conf=self.config_path)

        if mount_fs_name is not None:
            opts += ",mds_namespace={0}".format(mount_fs_name)

        for mount_opt in mount_options:
            opts += ",{0}".format(mount_opt)

        mount_cmd_args = ['sudo']
        if self.using_namespace:
            mount_cmd_args += ['nsenter',
                               '--net=/var/run/netns/{0}'.format(self.netns_name)]
        mount_cmd_args += ['./bin/mount.ceph',
                           ':{mount_path}'.format(mount_path=mount_path),
                           self.mountpoint, '-v', '-o', opts]
        self.client_remote.run(args=mount_cmd_args, timeout=(30*60),
                               omit_sudo=False)

        self.client_remote.run(
            args=['sudo', 'chmod', '1777', self.mountpoint], timeout=(5*60))

        self.mounted = True

    def cleanup_netns(self):
        if self.using_namespace:
            super(type(self), self).cleanup_netns()

    def _run_python(self, pyscript, py_version='python'):
        """
        Override this to remove the daemon-helper prefix that is used otherwise
        to make the process killable.
        """
        return self.client_remote.run(args=[py_version, '-c', pyscript],
                                      wait=False, stdout=StringIO())

class LocalFuseMount(FuseMount):
    def __init__(self, ctx, test_dir, client_id, brxnet):
        super(LocalFuseMount, self).__init__(ctx, None, test_dir, client_id, LocalRemote(), brxnet)

    @property
    def config_path(self):
        return "./ceph.conf"

    def get_keyring_path(self):
        # This is going to end up in a config file, so use an absolute path
        # to avoid assumptions about daemons' pwd
        return os.path.abspath("./client.{0}.keyring".format(self.client_id))

    def setupfs(self, name=None):
        if name is None and self.fs is not None:
            # Previous mount existed, reuse the old name
            name = self.fs.name
        self.fs = LocalFilesystem(self.ctx, name=name)
        log.info('Wait for MDS to reach steady state...')
        self.fs.wait_for_daemons()
        log.info('Ready to start {}...'.format(type(self).__name__))

    @property
    def _prefix(self):
        return BIN_PREFIX

    def _asok_path(self):
        # In teuthology, the asok is named after the PID of the ceph-fuse process, because it's
        # run foreground.  When running it daemonized however, the asok is named after
        # the PID of the launching process, not the long running ceph-fuse process.  Therefore
        # we need to give an exact path here as the logic for checking /proc/ for which
        # asok is alive does not work.

        # Load the asok path from ceph.conf as vstart.sh now puts admin sockets
        # in a tmpdir. All of the paths are the same, so no need to select
        # based off of the service type.
        d = "./out"
        with open(self.config_path) as f:
            for line in f:
                asok_conf = re.search("^\s*admin\s+socket\s*=\s*(.*?)[^/]+$", line)
                if asok_conf:
                    d = asok_conf.groups(1)[0]
                    break
        path = "{0}/client.{1}.*.asok".format(d, self.client_id)
        return path

    def mount(self, mount_path=None, mount_fs_name=None, mountpoint=None, mount_options=[]):
        if mountpoint is not None:
            self.mountpoint = mountpoint
        self.setupfs(name=mount_fs_name)
        if opt_use_ns:
            self.using_namespace = True
            self.setup_netns()
        else:
            self.using_namespace = False

        self.client_remote.run(args=['mkdir', '-p', self.mountpoint])

        def list_connections():
            self.client_remote.run(
                args=["mount", "-t", "fusectl", "/sys/fs/fuse/connections", "/sys/fs/fuse/connections"],
                check_status=False
            )

            p = self.client_remote.run(args=["ls", "/sys/fs/fuse/connections"],
                                       check_status=False, stdout=StringIO())
            if p.exitstatus != 0:
                log.warning("ls conns failed with {0}, assuming none".format(p.exitstatus))
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

        prefix = []
        if self.using_namespace:
            prefix += ['sudo', 'nsenter',
                       '--net=/var/run/netns/{0}'.format(self.netns_name),
                       '--setuid', str(os.getuid())]
        prefix += [os.path.join(BIN_PREFIX, "ceph-fuse")]
        if os.getuid() != 0:
            prefix += ["--client_die_on_failed_dentry_invalidate=false"]
        if mount_path is not None:
            prefix += ["--client_mountpoint={0}".format(mount_path)]
        if mount_fs_name is not None:
            prefix += ["--client_fs={0}".format(mount_fs_name)]
        prefix += mount_options;
        fuse_cmd_args = prefix + ["-f", "--name",
                                  "client.{0}".format(self.client_id),
                                  self.mountpoint]

        self.fuse_daemon = self.client_remote.run(args=fuse_cmd_args,
                                                  wait=False, omit_sudo=False)
        self._set_fuse_daemon_pid()
        log.info("Mounting client.{0} with pid "
                 "{1}".format(self.client_id, self.fuse_daemon.subproc.pid))

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

        self.gather_mount_info()

        self.mounted = True
    def _set_fuse_daemon_pid(self):
        # NOTE: When a command <args> is launched with sudo, two processes are
        # launched, one with sudo in <args> and other without. Make sure we
        # get the PID of latter one.
        with safe_while(sleep=1, tries=15) as proceed:
            while proceed():
                try:
                    sock = self.find_admin_socket()
                except (RuntimeError, CommandFailedError):
                    continue

                self.fuse_daemon.fuse_pid = int(re.match(".*\.(\d+)\.asok$",
                                                         sock).group(1))
                break

    def cleanup_netns(self):
        if self.using_namespace:
            super(type(self), self).cleanup_netns()

    def _run_python(self, pyscript, py_version='python'):
        """
        Override this to remove the daemon-helper prefix that is used otherwise
        to make the process killable.
        """
        return self.client_remote.run(args=[py_version, '-c', pyscript],
                                      wait=False, stdout=StringIO())

# XXX: this class has nothing to do with the Ceph daemon (ceph-mgr) of
# the same name.
class LocalCephManager(CephManager):
    def __init__(self):
        # Deliberately skip parent init, only inheriting from it to get
        # util methods like osd_dump that sit on top of raw_cluster_cmd
        self.controller = LocalRemote()

        # A minority of CephManager fns actually bother locking for when
        # certain teuthology tests want to run tasks in parallel
        self.lock = threading.RLock()

        self.log = lambda x: log.info(x)

        # Don't bother constructing a map of pools: it should be empty
        # at test cluster start, and in any case it would be out of date
        # in no time.  The attribute needs to exist for some of the CephManager
        # methods to work though.
        self.pools = {}

    def find_remote(self, daemon_type, daemon_id):
        """
        daemon_type like 'mds', 'osd'
        daemon_id like 'a', '0'
        """
        return LocalRemote()

    def run_ceph_w(self, watch_channel=None):
        """
        :param watch_channel: Specifies the channel to be watched.
                              This can be 'cluster', 'audit', ...
        :type watch_channel: str
        """
        args = [os.path.join(BIN_PREFIX, "ceph"), "-w"]
        if watch_channel is not None:
            args.append("--watch-channel")
            args.append(watch_channel)
        proc = self.controller.run(args=args, wait=False, stdout=StringIO())
        return proc

    def raw_cluster_cmd(self, *args, **kwargs):
        """
        args like ["osd", "dump"}
        return stdout string
        """
        proc = self.controller.run(args=[os.path.join(BIN_PREFIX, "ceph")] +\
                                   list(args), **kwargs, stdout=StringIO())
        return proc.stdout.getvalue()

    def raw_cluster_cmd_result(self, *args, **kwargs):
        """
        like raw_cluster_cmd but don't check status, just return rc
        """
        kwargs['check_status'] = False
        proc = self.controller.run(args=[os.path.join(BIN_PREFIX, "ceph")] + \
                                        list(args), **kwargs)
        return proc.exitstatus

    def admin_socket(self, daemon_type, daemon_id, command, check_status=True,
                     timeout=None, stdout=None):
        if stdout is None:
            stdout = StringIO()

        return self.controller.run(
            args=[os.path.join(BIN_PREFIX, "ceph"), "daemon",
                  "{0}.{1}".format(daemon_type, daemon_id)] + command,
            check_status=check_status, timeout=timeout, stdout=stdout)

    def get_mon_socks(self):
        """
        Get monitor sockets.

        :return socks: tuple of strings; strings are individual sockets.
        """
        from json import loads

        output = loads(self.raw_cluster_cmd('--format=json', 'mon', 'dump'))
        socks = []
        for mon in output['mons']:
            for addrvec_mem in mon['public_addrs']['addrvec']:
                socks.append(addrvec_mem['addr'])
        return tuple(socks)

    def get_msgrv1_mon_socks(self):
        """
        Get monitor sockets that use msgrv2 to operate.

        :return socks: tuple of strings; strings are individual sockets.
        """
        from json import loads

        output = loads(self.raw_cluster_cmd('--format=json', 'mon', 'dump'))
        socks = []
        for mon in output['mons']:
            for addrvec_mem in mon['public_addrs']['addrvec']:
                if addrvec_mem['type'] == 'v1':
                    socks.append(addrvec_mem['addr'])
        return tuple(socks)

    def get_msgrv2_mon_socks(self):
        """
        Get monitor sockets that use msgrv2 to operate.

        :return socks: tuple of strings; strings are individual sockets.
        """
        from json import loads

        output = loads(self.raw_cluster_cmd('--format=json', 'mon', 'dump'))
        socks = []
        for mon in output['mons']:
            for addrvec_mem in mon['public_addrs']['addrvec']:
                if addrvec_mem['type'] == 'v2':
                    socks.append(addrvec_mem['addr'])
        return tuple(socks)


class LocalCephCluster(CephCluster):
    def __init__(self, ctx):
        # Deliberately skip calling parent constructor
        self._ctx = ctx
        self.mon_manager = LocalCephManager()
        self._conf = defaultdict(dict)

    @property
    def admin_remote(self):
        return LocalRemote()

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


class LocalMDSCluster(LocalCephCluster, MDSCluster):
    def __init__(self, ctx):
        super(LocalMDSCluster, self).__init__(ctx)

        self.mds_ids = ctx.daemons.daemons['ceph.mds'].keys()
        self.mds_daemons = dict([(id_, LocalDaemon("mds", id_)) for id_ in self.mds_ids])

    def clear_firewall(self):
        # FIXME: unimplemented
        pass

    def newfs(self, name='cephfs', create=True):
        return LocalFilesystem(self._ctx, name=name, create=create)


class LocalMgrCluster(LocalCephCluster, MgrCluster):
    def __init__(self, ctx):
        super(LocalMgrCluster, self).__init__(ctx)

        self.mgr_ids = ctx.daemons.daemons['ceph.mgr'].keys()
        self.mgr_daemons = dict([(id_, LocalDaemon("mgr", id_)) for id_ in self.mgr_ids])


class LocalFilesystem(Filesystem, LocalMDSCluster):
    def __init__(self, ctx, fscid=None, name='cephfs', create=False, ec_profile=None):
        # Deliberately skip calling parent constructor
        self._ctx = ctx

        self.id = None
        self.name = name
        self.ec_profile = ec_profile
        self.metadata_pool_name = None
        self.metadata_overlay = False
        self.data_pool_name = None
        self.data_pools = None

        # Hack: cheeky inspection of ceph.conf to see what MDSs exist
        self.mds_ids = set()
        for line in open("ceph.conf").readlines():
            match = re.match("^\[mds\.(.+)\]$", line)
            if match:
                self.mds_ids.add(match.group(1))

        if not self.mds_ids:
            raise RuntimeError("No MDSs found in ceph.conf!")

        self.mds_ids = list(self.mds_ids)

        log.info("Discovered MDS IDs: {0}".format(self.mds_ids))

        self.mon_manager = LocalCephManager()

        self.mds_daemons = dict([(id_, LocalDaemon("mds", id_)) for id_ in self.mds_ids])

        self.client_remote = LocalRemote()

        self._conf = defaultdict(dict)

        if name is not None:
            if fscid is not None:
                raise RuntimeError("cannot specify fscid when creating fs")
            if create and not self.legacy_configured():
                self.create()
        else:
            if fscid is not None:
                self.id = fscid
                self.getinfo(refresh=True)

        # Stash a reference to the first created filesystem on ctx, so
        # that if someone drops to the interactive shell they can easily
        # poke our methods.
        if not hasattr(self._ctx, "filesystem"):
            self._ctx.filesystem = self

    @property
    def _prefix(self):
        return BIN_PREFIX

    def set_clients_block(self, blocked, mds_id=None):
        raise NotImplementedError()


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


def enumerate_methods(s):
    log.info("e: {0}".format(s))
    for t in s._tests:
        if isinstance(t, suite.BaseTestSuite):
            for sub in enumerate_methods(t):
                yield sub
        else:
            yield s, t


def load_tests(modules, loader):
    if modules:
        log.info("Executing modules: {0}".format(modules))
        module_suites = []
        for mod_name in modules:
            # Test names like cephfs.test_auto_repair
            module_suites.append(loader.loadTestsFromName(mod_name))
        log.info("Loaded: {0}".format(list(module_suites)))
        return suite.TestSuite(module_suites)
    else:
        log.info("Executing all cephfs tests")
        return loader.discover(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "cephfs")
        )


def scan_tests(modules):
    overall_suite = load_tests(modules, loader.TestLoader())

    max_required_mds = 0
    max_required_clients = 0
    max_required_mgr = 0
    require_memstore = False

    for suite_, case in enumerate_methods(overall_suite):
        max_required_mds = max(max_required_mds,
                               getattr(case, "MDSS_REQUIRED", 0))
        max_required_clients = max(max_required_clients,
                               getattr(case, "CLIENTS_REQUIRED", 0))
        max_required_mgr = max(max_required_mgr,
                               getattr(case, "MGRS_REQUIRED", 0))
        require_memstore = getattr(case, "REQUIRE_MEMSTORE", False) \
                               or require_memstore

    return max_required_mds, max_required_clients, \
            max_required_mgr, require_memstore


class LocalCluster(object):
    def __init__(self, rolename="placeholder"):
        self.remotes = {
            LocalRemote(): [rolename]
        }

    def only(self, requested):
        return self.__class__(rolename=requested)


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
            for svc_type in ["mon", "osd", "mds", "mgr"]:
                prefixed_type = "ceph." + svc_type
                if prefixed_type not in self.daemons.daemons:
                    self.daemons.daemons[prefixed_type] = {}
                match = re.match("^\[{0}\.(.+)\]$".format(svc_type), conf_line)
                if match:
                    svc_id = match.group(1)
                    self.daemons.daemons[prefixed_type][svc_id] = LocalDaemon(svc_type, svc_id)

    def __del__(self):
        test_path = self.teuthology_config['test_path']
        # opt_create_cluster_only does not create the test path
        if test_path:
            shutil.rmtree(test_path)

def teardown_cluster():
    log.info('\ntearing down the cluster...')
    remote.run(args=[os.path.join(SRC_PREFIX, "stop.sh")], timeout=60)
    remote.run(args=['rm', '-rf', './dev', './out'])

def clear_old_log():
    from os import stat

    try:
        stat(logpath)
    # would need an update when making this py3 compatible. Use FileNotFound
    # instead.
    except OSError:
        return
    else:
        os.remove(logpath)
        with open(logpath, 'w') as logfile:
            logfile.write('')
        init_log()
        log.info('logging in a fresh file now...')

def exec_test():
    # Parse arguments
    opt_interactive_on_error = False
    opt_create_cluster = False
    opt_create_cluster_only = False
    opt_ignore_missing_binaries = False
    opt_teardown_cluster = False
    global opt_log_ps_output
    opt_log_ps_output = False
    use_kernel_client = False
    global opt_use_ns
    opt_use_ns = False
    opt_brxnet= None

    args = sys.argv[1:]
    flags = [a for a in args if a.startswith("-")]
    modules = [a for a in args if not a.startswith("-")]
    for f in flags:
        if f == "--interactive":
            opt_interactive_on_error = True
        elif f == "--create":
            opt_create_cluster = True
        elif f == "--create-cluster-only":
            opt_create_cluster_only = True
        elif f == "--ignore-missing-binaries":
            opt_ignore_missing_binaries = True
        elif f == '--teardown':
            opt_teardown_cluster = True
        elif f == '--log-ps-output':
            opt_log_ps_output = True
        elif f == '--clear-old-log':
            clear_old_log()
        elif f == "--kclient":
            use_kernel_client = True
        elif f == '--usens':
            opt_use_ns = True
        elif '--brxnet' in f:
            if re.search(r'=[0-9./]+', f) is None:
                log.error("--brxnet=<ip/mask> option needs one argument: '{0}'".format(f))
                sys.exit(-1)
            opt_brxnet=f.split('=')[1]
            try:  
                IP(opt_brxnet)  
                if IP(opt_brxnet).iptype() == 'PUBLIC':
                    raise RuntimeError('is public')
            except Exception as e:
                log.error("Invalid ip '{0}' {1}".format(opt_brxnet, e))
                sys.exit(-1)
        else:
            log.error("Unknown option '{0}'".format(f))
            sys.exit(-1)

    # Help developers by stopping up-front if their tree isn't built enough for all the
    # tools that the tests might want to use (add more here if needed)
    require_binaries = ["ceph-dencoder", "cephfs-journal-tool", "cephfs-data-scan",
                        "cephfs-table-tool", "ceph-fuse", "rados", "cephfs-meta-injection"]
    missing_binaries = [b for b in require_binaries if not os.path.exists(os.path.join(BIN_PREFIX, b))]
    if missing_binaries and not opt_ignore_missing_binaries:
        log.error("Some ceph binaries missing, please build them: {0}".format(" ".join(missing_binaries)))
        sys.exit(-1)

    max_required_mds, max_required_clients, \
            max_required_mgr, require_memstore = scan_tests(modules)

    global remote
    remote = LocalRemote()

    # Tolerate no MDSs or clients running at start
    ps_txt = remote.run(args=["ps", "-u"+str(os.getuid())],
                        stdout=StringIO()).stdout.getvalue().strip()
    lines = ps_txt.split("\n")[1:]
    for line in lines:
        if 'ceph-fuse' in line or 'ceph-mds' in line:
            pid = int(line.split()[0])
            log.warning("Killing stray process {0}".format(line))
            os.kill(pid, signal.SIGKILL)

    # Fire up the Ceph cluster if the user requested it
    if opt_create_cluster or opt_create_cluster_only:
        log.info("Creating cluster with {0} MDS daemons".format(
            max_required_mds))
        teardown_cluster()
        vstart_env = os.environ.copy()
        vstart_env["FS"] = "0"
        vstart_env["MDS"] = max_required_mds.__str__()
        vstart_env["OSD"] = "4"
        vstart_env["MGR"] = max(max_required_mgr, 1).__str__()

        args = [os.path.join(SRC_PREFIX, "vstart.sh"), "-n", "-d",
                    "--nolockdep"]
        if require_memstore:
            args.append("--memstore")

        # usually, i get vstart.sh running completely in less than 100
        # seconds.
        remote.run(args=args, env=vstart_env, timeout=(3 * 60))

        # Wait for OSD to come up so that subsequent injectargs etc will
        # definitely succeed
        LocalCephCluster(LocalContext()).mon_manager.wait_for_all_osds_up(timeout=30)

    if opt_create_cluster_only:
        return

    # List of client mounts, sufficient to run the selected tests
    clients = [i.__str__() for i in range(0, max_required_clients)]

    test_dir = tempfile.mkdtemp()
    teuth_config['test_path'] = test_dir

    ctx = LocalContext()
    ceph_cluster = LocalCephCluster(ctx)
    mds_cluster = LocalMDSCluster(ctx)
    mgr_cluster = LocalMgrCluster(ctx)

    # Construct Mount classes
    mounts = []
    for client_id in clients:
        # Populate client keyring (it sucks to use client.admin for test clients
        # because it's awkward to find the logs later)
        client_name = "client.{0}".format(client_id)

        if client_name not in open("./keyring").read():
            p = remote.run(args=[os.path.join(BIN_PREFIX, "ceph"), "auth", "get-or-create", client_name,
                                 "osd", "allow rw",
                                 "mds", "allow",
                                 "mon", "allow r"], stdout=StringIO())

            open("./keyring", "at").write(p.stdout.getvalue())

        if use_kernel_client:
            mount = LocalKernelMount(ctx, test_dir, client_id, opt_brxnet)
        else:
            mount = LocalFuseMount(ctx, test_dir, client_id, opt_brxnet)

        mounts.append(mount)
        if os.path.exists(mount.mountpoint):
            if mount.is_mounted():
                log.warning("unmounting {0}".format(mount.mountpoint))
                mount.umount_wait()
            else:
                os.rmdir(mount.mountpoint)

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
        "ceph_cluster": ceph_cluster,
        "mds_cluster": mds_cluster,
        "mgr_cluster": mgr_cluster,
    })

    # For the benefit of polling tests like test_full -- in teuthology land we set this
    # in a .yaml, here it's just a hardcoded thing for the developer's pleasure.
    remote.run(args=[os.path.join(BIN_PREFIX, "ceph"), "tell", "osd.*", "injectargs", "--osd-mon-report-interval", "5"])
    ceph_cluster.set_ceph_conf("osd", "osd_mon_report_interval", "5")

    # Vstart defaults to two segments, which very easily gets a "behind on trimming" health warning
    # from normal IO latency.  Increase it for running teests.
    ceph_cluster.set_ceph_conf("mds", "mds log max segments", "10")

    # Make sure the filesystem created in tests has uid/gid that will let us talk to
    # it after mounting it (without having to  go root).  Set in 'global' not just 'mds'
    # so that cephfs-data-scan will pick it up too.
    ceph_cluster.set_ceph_conf("global", "mds root ino uid", "%s" % os.getuid())
    ceph_cluster.set_ceph_conf("global", "mds root ino gid", "%s" % os.getgid())

    # Monkeypatch get_package_version to avoid having to work out what kind of distro we're on
    def _get_package_version(remote, pkg_name):
        # Used in cephfs tests to find fuse version.  Your development workstation *does* have >=2.9, right?
        return "2.9"

    import teuthology.packaging
    teuthology.packaging.get_package_version = _get_package_version

    overall_suite = load_tests(modules, decorating_loader)

    # Filter out tests that don't lend themselves to interactive running,
    victims = []
    for case, method in enumerate_methods(overall_suite):
        fn = getattr(method, method._testMethodName)

        drop_test = False

        if hasattr(fn, 'is_for_teuthology') and getattr(fn, 'is_for_teuthology') is True:
            drop_test = True
            log.warning("Dropping test because long running: {method_id}".format(method_id=method.id()))

        if getattr(fn, "needs_trimming", False) is True:
            drop_test = (os.getuid() != 0)
            log.warning("Dropping test because client trim unavailable: {method_id}".format(method_id=method.id()))

        if drop_test:
            # Don't drop the test if it was explicitly requested in arguments
            is_named = False
            for named in modules:
                if named.endswith(method.id()):
                    is_named = True
                    break

            if not is_named:
                victims.append((case, method))

    log.info("Disabling {0} tests because of is_for_teuthology or needs_trimming".format(len(victims)))
    for s, method in victims:
        s._tests.remove(method)

    if opt_interactive_on_error:
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

    if opt_teardown_cluster:
        teardown_cluster()

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
