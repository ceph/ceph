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

Following are few important notes that might save some investigation around
vstart_runner.py -

* If using the FUSE client, ensure that the fuse package is installed and
  enabled on the system and that "user_allow_other" is added to /etc/fuse.conf.

* If using the kernel client, the user must have the ability to run commands
  with passwordless sudo access.

* A failure on the kernel client may crash the host, so it's recommended to
  use this functionality within a virtual machine.

* "adjust-ulimits", "ceph-coverage" and "sudo" in command arguments are
  overridden by vstart_runner.py. Former two usually have no applicability
  for test runs on developer's machines and see note point on "omit_sudo"
  to know more about overriding of "sudo".

* "omit_sudo" is re-set to False unconditionally in cases of commands
  "passwd" and "chown".

* The presence of binary file named after the first argument in the command
  arguments received by the method LocalRemote.run() is checked for in
  <ceph-repo-root>/build/bin/. If present, the first argument is replaced with
  the path to binary file.
"""

from io import StringIO
from json import loads
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
import unittest
import platform
import logging
from argparse import Namespace

from unittest import suite, loader

from teuthology.orchestra.run import quote, PIPE
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.orchestra.remote import RemoteShell
from teuthology.config import config as teuth_config
from teuthology.contextutil import safe_while
from teuthology.contextutil import MaxWhileTries
from teuthology.exceptions import CommandFailedError
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except:
    pass

def init_log(log_level=logging.INFO):
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
    log.setLevel(log_level)

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


def launch_subprocess(args, cwd=None, env=None, shell=True,
                      executable='/bin/bash'):
    return subprocess.Popen(args, cwd=cwd, env=env, shell=shell,
                            executable=executable, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, stdin=subprocess.PIPE)


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
    from tasks.ceph_manager import CephManager
    from tasks.cephfs.fuse_mount import FuseMount
    from tasks.cephfs.kernel_mount import KernelMount
    from tasks.cephfs.filesystem import Filesystem, MDSCluster, CephCluster
    from tasks.cephfs.mount import CephFSMount
    from tasks.mgr.mgr_test_case import MgrCluster
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

CEPH_CMD = os.path.join(BIN_PREFIX, 'ceph')
RADOS_CMD = os.path.join(BIN_PREFIX, 'rados')


def rm_nonascii_chars(var):
    var = var.replace(b'\xe2\x80\x98', b'\'')
    var = var.replace(b'\xe2\x80\x99', b'\'')
    return var

class LocalRemoteProcess(object):
    def __init__(self, args, subproc, check_status, stdout, stderr, usr_args):
        self.args = args
        self.subproc = subproc
        self.stdin = subproc.stdin
        self.stdout = stdout
        self.stderr = stderr
        self.usr_args = usr_args
        # this variable is meant for instance of this class named fuse_daemon.
        # child process of the command launched with sudo must be killed,
        # since killing parent process alone has no impact on the child
        # process.
        self.fuse_pid = -1

        self.check_status = check_status
        self.exitstatus = self.returncode = None

    def _write_stdout(self, out):
        if isinstance(self.stdout, StringIO):
            self.stdout.write(out.decode(errors='ignore'))
        elif self.stdout is None:
            pass
        else:
            self.stdout.write(out)

    def _write_stderr(self, err):
        if isinstance(self.stderr, StringIO):
            self.stderr.write(err.decode(errors='ignore'))
        elif self.stderr is None:
            pass
        else:
            self.stderr.write(err)

    def wait(self):
        # Null subproc.stdin so communicate() does not try flushing/closing it
        # again.
        if self.stdin is not None and self.stdin.closed:
            self.stdin = None
            self.subproc.stdin = None

        if self.finished:
            # Avoid calling communicate() on a dead process because it'll
            # give you stick about std* already being closed
            if self.check_status and self.exitstatus != 0:
                # TODO: print self.args or self.usr_args in exception msg?
                raise CommandFailedError(self.args, self.exitstatus)
            else:
                return

        out, err = self.subproc.communicate()
        out, err = rm_nonascii_chars(out), rm_nonascii_chars(err)
        self._write_stdout(out)
        self._write_stderr(err)

        self.exitstatus = self.returncode = self.subproc.returncode

        if self.exitstatus != 0:
            sys.stderr.write(out.decode())
            sys.stderr.write(err.decode())

        if self.check_status and self.exitstatus != 0:
            # TODO: print self.args or self.usr_args in exception msg?
            raise CommandFailedError(self.args, self.exitstatus)

    @property
    def finished(self):
        if self.exitstatus is not None:
            return True

        if self.subproc.poll() is not None:
            out, err = self.subproc.communicate()
            self._write_stdout(out)
            self._write_stderr(err)

            self.exitstatus = self.returncode = self.subproc.returncode

            return True
        else:
            return False

    def kill(self):
        log.debug("kill ")
        if self.subproc.pid and not self.finished:
            log.debug(f"kill: killing pid {self.subproc.pid} "
                      f"({self.usr_args})")
            if self.fuse_pid != -1:
                safe_kill(self.fuse_pid)
            else:
                safe_kill(self.subproc.pid)
        else:
            log.debug(f"kill: already terminated ({self.usr_args})")


def find_executable(exe):
    for path in os.getenv('PATH').split(':'):
        try:
            path = os.path.join(path, exe)
            os.lstat(path)
            return path
        except OSError:
            pass
    return None

class LocalRemote(RemoteShell):
    """
    Amusingly named class to present the teuthology RemoteProcess interface when we are really
    running things locally for vstart

    Run this inside your src/ dir!
    """

    rewrite_helper_tools = [
      {
        'name': 'adjust-ulimits',
        'path': None
      },
      {
        'name': 'daemon-helper',
        'path': None
      },
      {
        'name': 'stdin-killer',
        'path': None
      },
    ]

    def __init__(self):
        super().__init__()
        self.name = "local"
        self._hostname = "localhost"
        self.user = getpass.getuser()

    @property
    def hostname(self):
        if not hasattr(self, '_hostname'):
            self._hostname = 'localhost'
        return self._hostname

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

    def _expand_teuthology_tools(self, args):
        assert isinstance(args, list)
        for tool in self.rewrite_helper_tools:
            name, path = tool['name'], tool['path']
            if path is None:
                tool['path'] = find_executable(name)
                path = tool['path']
                log.info(f"{name} path is {path}")
            for i, arg in enumerate(args):
                if arg == name:
                    args[i] = path

    def _omit_cmd_args(self, args, omit_sudo):
        """
        Helper tools are omitted since those are not meant for tests executed
        using vstart_runner.py. And sudo's omission depends on the value of
        the variable omit_sudo.
        """
        helper_tools = ('ceph-coverage', 'None/archive/coverage')
        for i in helper_tools:
            if i in args:
                helper_tools_found = True
                break
        else:
            helper_tools_found = False

        if not helper_tools_found and 'sudo' not in args:
            return args, args

        prefix = ''

        if helper_tools_found:
            args = args.replace('None/archive/coverage', '')
            prefix += """
ceph-coverage() {
    "$@"
}
"""
            log.debug('Helper tools like adjust-ulimits and ceph-coverage '
                      'were omitted from the following cmd args before '
                      'logging and execution; check vstart_runner.py for '
                      'more details.')

        first_arg = args[ : args.find(' ')]
        # We'll let sudo be a part of command even omit flag says otherwise in
        # cases of commands which can normally be ran only by root.
        last_arg = args[args.rfind(' ') + 1 : ]
        # XXX: should sudo be omitted/allowed by default in cases similar to
        # that of "exec sudo" as well?
        if 'sudo' in args:
            for x in ('passwd', 'chown'):
                if x == first_arg or x == last_arg or f' {x} ' in args:
                    omit_sudo = False

        if omit_sudo:
            prefix += """
sudo() {
    "$@"
}
"""
            log.debug('"sudo" was omitted from the following cmd args '
                      'before execution and logging using function '
                      'overriding; check vstart_runner.py for more details.')

        # usr_args = args passed by the user/caller of this method
        usr_args, args = args, prefix + args

        return usr_args, args

    def _perform_checks_and_adjustments(self, args, omit_sudo):
        if isinstance(args, list):
            self._expand_teuthology_tools(args) # hack only for list
            args = quote(args)

        assert isinstance(args, str)

        first_arg = args[ : args.find(' ')]
        if '/' not in first_arg:
            local_bin = os.path.join(BIN_PREFIX, first_arg)
            if os.path.exists(local_bin):
                args = args.replace(first_arg, local_bin, 1)

        usr_args, args = self._omit_cmd_args(args, omit_sudo)

        # Let's print all commands on INFO log level since some logging level
        # might be changed to INFO from DEBUG during a vstart_runner.py's
        # execution due to code added for teuthology. This happened for
        # ceph_test_case.RunCephCmd.negtest_ceph_cmd(). Commands it executes
        # weren't printed in output because logging level for
        # ceph_test_case.py is set to INFO by default.
        log.info('> ' + usr_args)

        return args, usr_args

    # Wrapper to keep the interface exactly same as that of
    # teuthology.remote.run.
    def run(self, **kwargs):
        return self._do_run(**kwargs)

    # XXX: omit_sudo is set to True since using sudo can change the ownership
    # of files which becomes problematic for following executions of
    # vstart_runner.py.
    # XXX: omit_sudo is re-set to False even in cases of commands like passwd
    # and chown.
    # XXX: "adjust-ulimits", "ceph-coverage" and "sudo" in command arguments
    # are overridden. Former two usually have no applicability for test runs
    # on developer's machines and see note point on "omit_sudo" to know more
    # about overriding of "sudo".
    # XXX: the presence of binary file named after the first argument is
    # checked in build/bin/, if present the first argument is replaced with
    # the path to binary file.
    def _do_run(self, args, check_status=True, wait=True, stdout=None,
                stderr=None, cwd=None, stdin=None, logger=None, label=None,
                env=None, timeout=None, omit_sudo=True, shell=True, quiet=False):
        args, usr_args = self._perform_checks_and_adjustments(args, omit_sudo)

        subproc = launch_subprocess(args, cwd, env, shell)

        if stdin:
            # Hack: writing to stdin is not deadlock-safe, but it "always" works
            # as long as the input buffer is "small"
            if isinstance(stdin, str):
                subproc.stdin.write(stdin.encode())
            elif stdin == subprocess.PIPE or stdin == PIPE:
                pass
            elif isinstance(stdin, StringIO):
                subproc.stdin.write(bytes(stdin.getvalue(),encoding='utf8'))
            else:
                subproc.stdin.write(stdin.getvalue())

        proc = LocalRemoteProcess(
            args, subproc, check_status,
            stdout, stderr, usr_args
        )

        if wait:
            proc.wait()

        return proc

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
                log.debug("Found ps line for daemon: {0}".format(line))
                return int(line.split()[0])
        if not opt_log_ps_output:
            ps_txt = '(omitted)'
        log.debug("No match for {0} {1}: {2}".format(
            self.daemon_type, self.daemon_id, ps_txt))
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
        if pid is None:
            return
        log.debug("Killing PID {0} for {1}.{2}".format(pid, self.daemon_type, self.daemon_id))
        os.kill(pid, signal.SIGTERM)

        waited = 0
        while pid is not None:
            new_pid = self._get_pid()
            if new_pid is not None and new_pid != pid:
                log.debug("Killing new PID {0}".format(new_pid))
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
            os.path.join(BIN_PREFIX, "ceph-{0}".format(self.daemon_type)),
            "-i", self.daemon_id])

    def signal(self, sig, silent=False):
        if not self.running():
            raise RuntimeError("Can't send signal to non-running daemon")

        os.kill(self._get_pid(), sig)
        if not silent:
            log.debug("Sent signal {0} to {1}.{2}".format(sig, self.daemon_type, self.daemon_id))


def safe_kill(pid):
    """
    os.kill annoyingly raises exception if process already dead.  Ignore it.
    """
    try:
        return remote.run(args=f'sudo kill -{signal.SIGKILL.value} {pid}',
                          omit_sudo=False)
    except OSError as e:
        if e.errno == errno.ESRCH:
            # Raced with process termination
            pass
        else:
            raise

def mon_in_localhost(config_path="./ceph.conf"):
    """
    If the ceph cluster is using the localhost IP as mon host, will must disable ns unsharing
    """
    with open(config_path) as f:
        for line in f:
            local = re.match(r'^\s*mon host\s*=\s*\[((v1|v2):127\.0\.0\.1:\d+,?)+\]', line)
            if local:
                return True
    return False

class LocalCephFSMount():
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

    @property
    def _prefix(self):
        return BIN_PREFIX

    def _asok_path(self):
        # In teuthology, the asok is named after the PID of the ceph-fuse
        # process, because it's run foreground.  When running it daemonized
        # however, the asok is named after the PID of the launching process,
        # not the long running ceph-fuse process.  Therefore we need to give
        # an exact path here as the logic for checking /proc/ for which asok
        # is alive does not work.

        # Load the asok path from ceph.conf as vstart.sh now puts admin sockets
        # in a tmpdir. All of the paths are the same, so no need to select
        # based off of the service type.
        d = "./asok"
        with open(self.config_path) as f:
            for line in f:
                asok_conf = re.search("^\s*admin\s+socket\s*=\s*(.*?)[^/]+$", line)
                if asok_conf:
                    d = asok_conf.groups(1)[0]
                    break
        path = "{0}/client.{1}.*.asok".format(d, self.client_id)
        return path

    def setup_netns(self):
        if opt_use_ns:
            super(type(self), self).setup_netns()

    @property
    def _nsenter_args(self):
            if opt_use_ns:
                return super(type(self), self)._nsenter_args
            else:
                return []

    def setupfs(self, name=None):
        if name is None and self.fs is not None:
            # Previous mount existed, reuse the old name
            name = self.fs.name
        self.fs = LocalFilesystem(self.ctx, name=name)
        log.info('Wait for MDS to reach steady state...')
        self.fs.wait_for_daemons()
        log.info('Ready to start {}...'.format(type(self).__name__))

    def is_blocked(self):
        self.fs = LocalFilesystem(self.ctx, name=self.cephfs_name)

        output = self.fs.mon_manager.raw_cluster_cmd(args='osd blocklist ls')
        return self.addr in output


class LocalKernelMount(LocalCephFSMount, KernelMount):
    def __init__(self, ctx, test_dir, client_id=None,
                 client_keyring_path=None, client_remote=None,
                 hostfs_mntpt=None, cephfs_name=None, cephfs_mntpt=None,
                 brxnet=None):
        super(LocalKernelMount, self).__init__(ctx=ctx, test_dir=test_dir,
            client_id=client_id, client_keyring_path=client_keyring_path,
            client_remote=LocalRemote(), hostfs_mntpt=hostfs_mntpt,
            cephfs_name=cephfs_name, cephfs_mntpt=cephfs_mntpt, brxnet=brxnet)

        # Make vstart_runner compatible with teuth and qa/tasks/cephfs.
        self._mount_bin = [os.path.join(BIN_PREFIX , 'mount.ceph')]

    def get_global_addr(self):
        self.get_global_inst()
        self.addr = self.inst[self.inst.find(' ') + 1 : ]
        return self.addr

    def get_global_inst(self):
        clients = self.client_remote.run(
            args=f'{CEPH_CMD} tell mds.* session ls',
            stdout=StringIO()).stdout.getvalue()
        clients = loads(clients)
        for c in clients:
            if c['id'] == self.id:
                self.inst = c['inst']
                return self.inst


class LocalFuseMount(LocalCephFSMount, FuseMount):
    def __init__(self, ctx, test_dir, client_id, client_keyring_path=None,
                 client_remote=None, hostfs_mntpt=None, cephfs_name=None,
                 cephfs_mntpt=None, brxnet=None):
        super(LocalFuseMount, self).__init__(ctx=ctx, test_dir=test_dir,
            client_id=client_id, client_keyring_path=client_keyring_path,
            client_remote=LocalRemote(), hostfs_mntpt=hostfs_mntpt,
            cephfs_name=cephfs_name, cephfs_mntpt=cephfs_mntpt, brxnet=brxnet)

        # Following block makes tests meant for teuthology compatible with
        # vstart_runner.
        self._mount_bin = [os.path.join(BIN_PREFIX, 'ceph-fuse')]
        self._mount_cmd_cwd, self._mount_cmd_logger, \
            self._mount_cmd_stdin = None, None, None

    # XXX: CephFSMount._create_mntpt() sets mountpoint's permission mode to
    # 0000 which doesn't work for vstart_runner since superuser privileges are
    # not used for mounting Ceph FS with FUSE.
    def _create_mntpt(self):
        self.client_remote.run(args=f'mkdir -p -v {self.hostfs_mntpt}')

    def _run_mount_cmd(self, mntopts, mntargs, check_status):
        retval = super(type(self), self)._run_mount_cmd(mntopts, mntargs,
                                                        check_status)
        if retval is None: # None represents success
            self._set_fuse_daemon_pid(check_status)
        return retval

    def _get_mount_cmd(self, mntopts, mntargs):
        mount_cmd = super(type(self), self)._get_mount_cmd(mntopts, mntargs)

        if os.getuid() != 0:
            mount_cmd += ['--client_die_on_failed_dentry_invalidate=false']
            # XXX: Passing ceph-fuse option above makes using sudo command
            # redunant. Plus, we prefer that vstart_runner.py doesn't use sudo
            # command as far as possbile.
            mount_cmd.remove('sudo')

        return mount_cmd

    @property
    def _fuse_conn_check_timeout(self):
        return 30

    def _add_valgrind_args(self, mount_cmd):
        return []

    def _set_fuse_daemon_pid(self, check_status):
        # NOTE: When a command <args> is launched with sudo, two processes are
        # launched, one with sudo in <args> and other without. Make sure we
        # get the PID of latter one.
        try:
            with safe_while(sleep=1, tries=15) as proceed:
                while proceed():
                    try:
                        sock = self.find_admin_socket()
                    except (RuntimeError, CommandFailedError):
                        continue

                    self.fuse_daemon.fuse_pid = int(re.match(".*\.(\d+)\.asok$",
                                                             sock).group(1))
                    break
        except MaxWhileTries:
            if check_status:
                raise
            else:
                pass

# XXX: this class has nothing to do with the Ceph daemon (ceph-mgr) of
# the same name.
class LocalCephManager(CephManager):
    def __init__(self, ctx=None):
        self.ctx = ctx
        if self.ctx:
            self.cluster = self.ctx.config['cluster']

        # Deliberately skip parent init, only inheriting from it to get
        # util methods like osd_dump that sit on top of raw_cluster_cmd
        self.controller = LocalRemote()

        # A minority of CephManager fns actually bother locking for when
        # certain teuthology tests want to run tasks in parallel
        self.lock = threading.RLock()

        self.log = lambda x: log.debug(x)

        # Don't bother constructing a map of pools: it should be empty
        # at test cluster start, and in any case it would be out of date
        # in no time.  The attribute needs to exist for some of the CephManager
        # methods to work though.
        self.pools = {}

        # NOTE: These variables are being overriden here so that parent class
        # can pick it up.
        self.cephadm = False
        self.rook = False
        self.testdir = None
        self.RADOS_CMD = [RADOS_CMD]

    def get_ceph_cmd(self, **kwargs):
        return [CEPH_CMD]

    def find_remote(self, daemon_type, daemon_id):
        """
        daemon_type like 'mds', 'osd'
        daemon_id like 'a', '0'
        """
        return LocalRemote()

    def admin_socket(self, daemon_type, daemon_id, command, check_status=True,
                     timeout=None, stdout=None):
        if stdout is None:
            stdout = StringIO()

        args=[CEPH_CMD, "daemon", f"{daemon_type}.{daemon_id}"] + command
        return self.controller.run(args=args, check_status=check_status,
                                   timeout=timeout, stdout=stdout)


class LocalCephCluster(CephCluster):
    def __init__(self, ctx):
        # Deliberately skip calling CephCluster constructor
        self._ctx = ctx
        self.mon_manager = LocalCephManager(ctx=self._ctx)
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
                log.debug("Searching for existing instance {0}/{1}".format(
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
                        log.debug("Found string to replace at {0}".format(
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
        LocalCephCluster.__init__(self, ctx)
        # Deliberately skip calling MDSCluster constructor
        self._mds_ids = ctx.daemons.daemons['ceph.mds'].keys()
        log.debug("Discovered MDS IDs: {0}".format(self._mds_ids))
        self._mds_daemons = dict([(id_, LocalDaemon("mds", id_)) for id_ in self.mds_ids])

    @property
    def mds_ids(self):
        return self._mds_ids

    @property
    def mds_daemons(self):
        return self._mds_daemons

    def clear_firewall(self):
        # FIXME: unimplemented
        pass

    def newfs(self, name='cephfs', create=True):
        return LocalFilesystem(self._ctx, name=name, create=create)

    def delete_all_filesystems(self):
        """
        Remove all filesystems that exist, and any pools in use by them.
        """
        for fs in self.status().get_filesystems():
            LocalFilesystem(ctx=self._ctx, fscid=fs['id']).destroy()


class LocalMgrCluster(LocalCephCluster, MgrCluster):
    def __init__(self, ctx):
        super(LocalMgrCluster, self).__init__(ctx)

        self.mgr_ids = ctx.daemons.daemons['ceph.mgr'].keys()
        self.mgr_daemons = dict([(id_, LocalDaemon("mgr", id_)) for id_ in self.mgr_ids])


class LocalFilesystem(LocalMDSCluster, Filesystem):
    def __init__(self, ctx, fs_config={}, fscid=None, name=None, create=False):
        # Deliberately skip calling Filesystem constructor
        LocalMDSCluster.__init__(self, ctx)

        self.id = None
        self.name = name
        self.metadata_pool_name = None
        self.metadata_overlay = False
        self.data_pool_name = None
        self.data_pools = None
        self.fs_config = fs_config
        self.ec_profile = fs_config.get('ec_profile')

        self.mon_manager = LocalCephManager(ctx=self._ctx)

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


class LocalCluster(object):
    def __init__(self, rolename="placeholder"):
        self.remotes = {
            LocalRemote(): [rolename]
        }

    def only(self, requested):
        return self.__class__(rolename=requested)

    def run(self, *args, **kwargs):
        r = []
        for remote in self.remotes.keys():
            r.append(remote.run(*args, **kwargs))
        return r


class LocalContext(object):
    def __init__(self):
        FSID = remote.run(args=[os.path.join(BIN_PREFIX, 'ceph'), 'fsid'],
                          stdout=StringIO()).stdout.getvalue()

        cluster_name = 'ceph'
        self.archive = "./"
        self.config = {'cluster': cluster_name}
        self.ceph = {cluster_name: Namespace()}
        self.ceph[cluster_name].fsid = FSID
        self.teuthology_config = teuth_config
        self.cluster = LocalCluster()
        self.daemons = DaemonGroup()
        if not hasattr(self, 'managers'):
            self.managers = {}
        self.managers[self.config['cluster']] = LocalCephManager(ctx=self)

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


#########################################
#
# stuff necessary for launching tests...
#
#########################################


def enumerate_methods(s):
    log.debug("e: {0}".format(s))
    for t in s._tests:
        if isinstance(t, suite.BaseTestSuite):
            for sub in enumerate_methods(t):
                yield sub
        else:
            yield s, t


def load_tests(modules, loader):
    if modules:
        log.debug("Executing modules: {0}".format(modules))
        module_suites = []
        for mod_name in modules:
            # Test names like cephfs.test_auto_repair
            module_suites.append(loader.loadTestsFromName(mod_name))
        log.debug("Loaded: {0}".format(list(module_suites)))
        return suite.TestSuite(module_suites)
    else:
        log.debug("Executing all cephfs tests")
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


class LogRotate():
    def __init__(self):
        self.conf_file_path = os.path.join(os.getcwd(), 'logrotate.conf')
        self.state_file_path = os.path.join(os.getcwd(), 'logrotate.state')

    def run_logrotate(self):
        remote.run(args=['logrotate', '-f', self.conf_file_path, '-s',
                         self.state_file_path, '--verbose'])


def teardown_cluster():
    log.info('\ntearing down the cluster...')
    try:
        remote.run(args=[os.path.join(SRC_PREFIX, "stop.sh")], timeout=60)
    except CommandFailedError as e:
        log.error('stop.sh failed: %s', e)
    log.info('\nceph cluster torn down')
    remote.run(args=['rm', '-rf', './dev', './out'])


def clear_old_log():
    try:
        os.stat(logpath)
    except FileNotFoundError:
        return
    else:
        os.remove(logpath)
        with open(logpath, 'w') as logfile:
            logfile.write('')
        init_log(log.level)
        log.debug('logging in a fresh file now...')


class LogStream(object):
    def __init__(self):
        self.buffer = ""
        self.omit_result_lines = False

    def _del_result_lines(self):
        """
        Don't let unittest.TextTestRunner print "Ran X tests in Ys",
        vstart_runner.py will do it for itself since it runs tests in a
        testsuite one by one.
        """
        if self.omit_result_lines:
            self.buffer = re.sub('-'*70+'\nran [0-9]* test in [0-9.]*s\n*',
                                 '', self.buffer, flags=re.I)
        self.buffer = re.sub('failed \(failures=[0-9]*\)\n', '', self.buffer,
                             flags=re.I)
        self.buffer = self.buffer.replace('OK\n', '')

    def write(self, data):
        self.buffer += data
        if self.buffer.count("\n") > 5:
            self._write()

    def _write(self):
        if opt_rotate_logs:
            self._del_result_lines()
        if self.buffer == '':
            return

        lines = self.buffer.split("\n")
        for line in lines:
            # sys.stderr.write(line + "\n")
            log.info(line)
        self.buffer = ''

    def flush(self):
        pass

    def __del__(self):
        self._write()


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


# XXX: class we require would be inherited from this one and one of
# InteractiveFailureResult and unittestunittest.TextTestResult.
class LoggingResultTemplate(object):
    fail_on_skip = False

    def startTest(self, test):
        log.info("Starting test: {0}".format(self.getDescription(test)))
        test.started_at = datetime.datetime.utcnow()
        return super(LoggingResultTemplate, self).startTest(test)

    def stopTest(self, test):
        log.info("Stopped test: {0} in {1}s".format(
            self.getDescription(test),
            (datetime.datetime.utcnow() - test.started_at).total_seconds()
        ))

    def addSkip(self, test, reason):
        if LoggingResultTemplate.fail_on_skip:
            # Don't just call addFailure because that requires a traceback
            self.failures.append((test, reason))
        else:
            super(LoggingResultTemplate, self).addSkip(test, reason)


def launch_tests(overall_suite):
    if opt_rotate_logs or not opt_exit_on_test_failure:
        return launch_individually(overall_suite)
    else:
        return launch_entire_suite(overall_suite)


def get_logging_result_class():
    result_class = InteractiveFailureResult if opt_interactive_on_error else \
        unittest.TextTestResult
    return type('', (LoggingResultTemplate, result_class), {})


def launch_individually(overall_suite):
    no_of_tests_execed = 0
    no_of_tests_failed, no_of_tests_execed = 0, 0
    LoggingResult = get_logging_result_class()
    stream = LogStream()
    stream.omit_result_lines = True
    if opt_rotate_logs:
        logrotate = LogRotate()

    started_at = datetime.datetime.utcnow()
    for suite_, case in enumerate_methods(overall_suite):
        # don't run logrotate beforehand since some ceph daemons might be
        # down and pre/post-rotate scripts in logrotate.conf might fail.
        if opt_rotate_logs:
            logrotate.run_logrotate()

        result = unittest.TextTestRunner(stream=stream,
                                         resultclass=LoggingResult,
                                         verbosity=2, failfast=True).run(case)

        if not result.wasSuccessful():
            if opt_exit_on_test_failure:
                break
            else:
                no_of_tests_failed += 1

        no_of_tests_execed += 1
    time_elapsed = (datetime.datetime.utcnow() - started_at).total_seconds()

    if result.wasSuccessful():
        log.info('')
        log.info('-'*70)
        log.info(f'Ran {no_of_tests_execed} tests successfully in '
                 f'{time_elapsed}s')
        if no_of_tests_failed > 0:
            log.info(f'{no_of_tests_failed} tests failed')
        log.info('')
        log.info('OK')

    return result


def launch_entire_suite(overall_suite):
    LoggingResult = get_logging_result_class()

    testrunner = unittest.TextTestRunner(stream=LogStream(),
                                         resultclass=LoggingResult,
                                         verbosity=2, failfast=True)
    return testrunner.run(overall_suite)


def exec_test():
    # Parse arguments
    global opt_interactive_on_error
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
    opt_verbose = True
    global opt_rotate_logs
    opt_rotate_logs = False
    global opt_exit_on_test_failure
    opt_exit_on_test_failure = True

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
        elif '--no-verbose' == f:
            opt_verbose = False
        elif f == '--rotate-logs':
            opt_rotate_logs = True
        elif f == '--run-all-tests':
            opt_exit_on_test_failure = False
        elif f == '--debug':
            log.setLevel(logging.DEBUG)
        else:
            log.error("Unknown option '{0}'".format(f))
            sys.exit(-1)

    # Help developers by stopping up-front if their tree isn't built enough for all the
    # tools that the tests might want to use (add more here if needed)
    require_binaries = ["ceph-dencoder", "cephfs-journal-tool", "cephfs-data-scan",
                        "cephfs-table-tool", "ceph-fuse", "rados", "cephfs-meta-injection"]
    # What binaries may be required is task specific
    require_binaries = ["ceph-dencoder",  "rados"]
    missing_binaries = [b for b in require_binaries if not os.path.exists(os.path.join(BIN_PREFIX, b))]
    if missing_binaries and not opt_ignore_missing_binaries:
        log.error("Some ceph binaries missing, please build them: {0}".format(" ".join(missing_binaries)))
        sys.exit(-1)

    max_required_mds, max_required_clients, \
            max_required_mgr, require_memstore = scan_tests(modules)

    global remote
    remote = LocalRemote()

    CephFSMount.cleanup_stale_netnses_and_bridge(remote)

    # Tolerate no MDSs or clients running at start
    ps_txt = remote.run(args=["ps", "-u"+str(os.getuid())],
                        stdout=StringIO()).stdout.getvalue().strip()
    lines = ps_txt.split("\n")[1:]
    for line in lines:
        if 'ceph-fuse' in line or 'ceph-mds' in line:
            pid = int(line.split()[0])
            log.warning("Killing stray process {0}".format(line))
            remote.run(args=f'sudo kill -{signal.SIGKILL.value} {pid}',
                       omit_sudo=False)

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

        args = [
            os.path.join(SRC_PREFIX, "vstart.sh"),
            "-n",
            "--nolockdep",
        ]
        if require_memstore:
            args.append("--memstore")

        if opt_verbose:
            args.append("-d")

        log.info('\nrunning vstart.sh now...')
        # usually, i get vstart.sh running completely in less than 100
        # seconds.
        remote.run(args=args, env=vstart_env, timeout=(3 * 60))
        log.info('\nvstart.sh finished running')

        # Wait for OSD to come up so that subsequent injectargs etc will
        # definitely succeed
        LocalCephCluster(LocalContext()).mon_manager.wait_for_all_osds_up(timeout=30)

    if opt_create_cluster_only:
        return

    if opt_use_ns and mon_in_localhost() and not opt_create_cluster:
        raise RuntimeError("cluster is on localhost; '--usens' option is incompatible. Or you can pass an extra '--create' option to create a new cluster without localhost!")

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
            p = remote.run(args=[CEPH_CMD, "auth", "get-or-create", client_name,
                                 "osd", "allow rw",
                                 "mds", "allow",
                                 "mon", "allow r"], stdout=StringIO())

            open("./keyring", "at").write(p.stdout.getvalue())

        if use_kernel_client:
            mount = LocalKernelMount(ctx=ctx, test_dir=test_dir,
                                     client_id=client_id, brxnet=opt_brxnet)
        else:
            mount = LocalFuseMount(ctx=ctx, test_dir=test_dir,
                                   client_id=client_id, brxnet=opt_brxnet)

        mounts.append(mount)
        if os.path.exists(mount.hostfs_mntpt):
            if mount.is_mounted():
                log.warning("unmounting {0}".format(mount.hostfs_mntpt))
                mount.umount_wait()
            else:
                os.rmdir(mount.hostfs_mntpt)

    from tasks.cephfs_test_runner import DecoratingLoader

    decorating_loader = DecoratingLoader({
        "ctx": ctx,
        "mounts": mounts,
        "ceph_cluster": ceph_cluster,
        "mds_cluster": mds_cluster,
        "mgr_cluster": mgr_cluster,
    })

    # For the benefit of polling tests like test_full -- in teuthology land we set this
    # in a .yaml, here it's just a hardcoded thing for the developer's pleasure.
    remote.run(args=[CEPH_CMD, "tell", "osd.*", "injectargs", "--osd-mon-report-interval", "5"])
    ceph_cluster.set_ceph_conf("osd", "osd_mon_report_interval", "5")

    # Enable override of recovery options if mClock scheduler is active. This is to allow
    # current and future tests to modify recovery related limits. This is because by default,
    # with mclock enabled, a subset of recovery options are not allowed to be modified.
    remote.run(args=[CEPH_CMD, "tell", "osd.*", "injectargs", "--osd-mclock-override-recovery-settings", "true"])
    ceph_cluster.set_ceph_conf("osd", "osd_mclock_override_recovery_settings", "true")

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

    log.debug("Disabling {0} tests because of is_for_teuthology or needs_trimming".format(len(victims)))
    for s, method in victims:
        s._tests.remove(method)

    overall_suite = load_tests(modules, loader.TestLoader())
    result = launch_tests(overall_suite)

    CephFSMount.cleanup_stale_netnses_and_bridge(remote)
    if opt_teardown_cluster:
        teardown_cluster()

    if not result.wasSuccessful():
        # no point in duplicating if we can have multiple failures in same
        # run.
        if opt_exit_on_test_failure:
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
