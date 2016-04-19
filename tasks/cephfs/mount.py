from contextlib import contextmanager
import json
import logging
import datetime
import time
from textwrap import dedent
import os
from StringIO import StringIO
from teuthology.orchestra import run
from teuthology.orchestra.run import CommandFailedError, ConnectionLostError

log = logging.getLogger(__name__)


class CephFSMount(object):
    def __init__(self, test_dir, client_id, client_remote):
        """
        :param test_dir: Global teuthology test dir
        :param client_id: Client ID, the 'foo' in client.foo
        :param client_remote: Remote instance for the host where client will run
        """

        self.test_dir = test_dir
        self.client_id = client_id
        self.client_remote = client_remote
        self.mountpoint_dir_name = 'mnt.{id}'.format(id=self.client_id)

        self.test_files = ['a', 'b', 'c']

        self.background_procs = []

    @property
    def mountpoint(self):
        return os.path.join(
            self.test_dir, '{dir_name}'.format(dir_name=self.mountpoint_dir_name))

    def is_mounted(self):
        raise NotImplementedError()

    def mount(self, mount_path=None):
        raise NotImplementedError()

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

    def kill_cleanup(self):
        raise NotImplementedError()

    def kill(self):
        raise NotImplementedError()

    def cleanup(self):
        raise NotImplementedError()

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
    def mounted(self):
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

    def create_files(self):
        assert(self.is_mounted())

        for suffix in self.test_files:
            log.info("Creating file {0}".format(suffix))
            self.client_remote.run(args=[
                'sudo', 'touch', os.path.join(self.mountpoint, suffix)
            ])

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

    def _run_python(self, pyscript):
        return self.client_remote.run(args=[
            'sudo', 'adjust-ulimits', 'daemon-helper', 'kill', 'python', '-c', pyscript
        ], wait=False, stdin=run.PIPE, stdout=StringIO())

    def run_python(self, pyscript):
        p = self._run_python(pyscript)
        p.wait()
        return p.stdout.getvalue().strip()

    def run_shell(self, args, wait=True):
        args = ["cd", self.mountpoint, run.Raw('&&'), "sudo"] + args
        return self.client_remote.run(args=args, stdout=StringIO(), wait=wait)

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

    def open_background(self, basename="background_file"):
        """
        Open a file for writing, then block such that the client
        will hold a capability
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        pyscript = dedent("""
            import time

            f = open("{path}", 'w')
            f.write('content')
            f.flush()
            f.write('content2')
            while True:
                time.sleep(1)
            """).format(path=path)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

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

        log.info("lock file {0}".format(basename))
        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

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
            except IOError, e:
                if e.errno == errno.EAGAIN:
                    pass
            else:
                raise RuntimeError("flock on file {path}-1 not found")"""
        script_builder += """
            f2 = open("{path}-2", 'r')
            try:
                lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
                fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            except IOError, e:
                if e.errno == errno.EAGAIN:
                    pass
            else:
                raise RuntimeError("posix lock on file {path}-2 not found")
            """
        pyscript = dedent(script_builder).format(path=path)

        log.info("check lock on file {0}".format(basename))
        self.client_remote.run(args=[
            'sudo', 'python', '-c', pyscript
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

            fd = os.open("{path}", os.O_RDWR | os.O_CREAT, 0644)
            try:
                while True:
                    os.write(fd, 'content')
                    time.sleep(1)
                    if not {loop}:
                        break
            except IOError, e:
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
                               "count={0}".format(n_mb),
                               "seek={0}".format(seek)
                               ], wait=wait)

    def write_test_pattern(self, filename, size):
        log.info("Writing {0} bytes to {1}".format(size, filename))
        return self.run_python(dedent("""
            import zlib
            path = "{path}"
            f = open(path, 'w')
            for i in range(0, {size}):
                val = zlib.crc32("%s" % i) & 7
                f.write(chr(val))
            f.close()
        """.format(
            path=os.path.join(self.mountpoint, filename),
            size=size
        )))

    def validate_test_pattern(self, filename, size):
        log.info("Validating {0} bytes from {1}".format(size, filename))
        return self.run_python(dedent("""
            import zlib
            path = "{path}"
            f = open(path, 'r')
            bytes = f.read()
            f.close()
            if len(bytes) != {size}:
                raise RuntimeError("Bad length {{0}} vs. expected {{1}}".format(
                    len(bytes), {size}
                ))
            for i, b in enumerate(bytes):
                val = zlib.crc32("%s" % i) & 7
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
                h = open(fname, 'w')
                h.write('content')
                if {sync}:
                    h.flush()
                    os.fsync(h.fileno())
                h.close()
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

    def spam_dir_background(self, path):
        """
        Create directory `path` and do lots of metadata operations
        in it until further notice.
        """
        assert(self.is_mounted())
        abs_path = os.path.join(self.mountpoint, path)

        pyscript = dedent("""
            import sys
            import time
            import os

            abs_path = "{abs_path}"

            if not os.path.exists(abs_path):
                os.makedirs(abs_path)

            n = 0
            while True:
                file_path = os.path.join(abs_path, "tmp%d" % n)
                f = open(file_path, 'w')
                f.close()
                n = n + 1
            """).format(abs_path=abs_path)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def get_global_id(self):
        raise NotImplementedError()

    def get_osd_epoch(self):
        raise NotImplementedError()

    def stat(self, fs_path, wait=True):
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

        pyscript = dedent("""
            import os
            import stat
            import json
            import sys

            try:
                s = os.stat("{path}")
            except OSError as e:
                sys.exit(e.errno)

            attrs = ["st_mode", "st_ino", "st_dev", "st_nlink", "st_uid", "st_gid", "st_size", "st_atime", "st_mtime", "st_ctime"]
            print json.dumps(
                dict([(a, getattr(s, a)) for a in attrs]),
                indent=2)
            """).format(path=abs_path)
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

    def path_to_ino(self, fs_path):
        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import os
            import stat

            print os.stat("{path}").st_ino
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

    def getfattr(self, path, attr):
        """
        Wrap getfattr: return the values of a named xattr on one file.

        :return: a string
        """
        p = self.run_shell(["getfattr", "--only-values", "-n", attr, path])
        return p.stdout.getvalue()
