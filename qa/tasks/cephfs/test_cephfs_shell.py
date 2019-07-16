import os
import crypt
import logging
from tempfile import mkstemp as tempfile_mkstemp
from StringIO import StringIO
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestCephFSShell(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    def _cephfs_shell(self, cmd, opts=None, stdin=None):
        args = ["cephfs-shell", "-c", self.mount_a.config_path]
        if opts is not None:
            args.extend(opts)
        args.extend(("--", cmd))
        log.info("Running command: {}".format(" ".join(args)))
        status = self.mount_a.client_remote.run(args=args,
                                                stdout=StringIO(),
                                                stdin=stdin)
        return status.stdout.getvalue().strip()

    def get_cephfs_shell_script_output(self, script, mount_x=None, stdin=None):
        return self.run_cephfs_shell_script(script, mount_x, stdin).stdout.\
            getvalue().strip()

    def run_cephfs_shell_script(self, script, mount_x=None, stdin=None):
        if mount_x is None:
            mount_x = self.mount_a

        scriptpath = tempfile_mkstemp(prefix='test-cephfs', text=True)[1]
        with open(scriptpath, 'w') as scriptfile:
            scriptfile.write(script)
        # copy script to the machine running cephfs-shell.
        mount_x.client_remote.put_file(scriptpath, scriptpath)
        mount_x.run_shell('chmod 755 ' + scriptpath)

        args = ["cephfs-shell", "-c", mount_x.config_path, '-b', scriptpath]
        log.info('Running script \"' + scriptpath + '\"')
        return mount_x.client_remote.run(args=args, stdout=StringIO(),
                                         stderr=StringIO(), stdin=stdin)

class TestMkdir(TestCephFSShell):
    def test_mkdir(self):
        """
        Test that mkdir creates directory
        """
        o = self._cephfs_shell("mkdir d1")
        log.info("cephfs-shell output:\n{}".format(o))

        o = self.mount_a.stat('d1')
        log.info("mount_a output:\n{}".format(o))

    def test_mkdir_with_07000_octal_mode(self):
        """
        Test that mkdir fails with octal mode greater than 0777
        """
        o = self._cephfs_shell("mkdir -m 07000 d2")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d2 should fail
        try:
            o = self.mount_a.stat('d2')
            log.info("mount_a output:\n{}".format(o))
        except:
            pass

    def test_mkdir_with_negative_octal_mode(self):
        """
        Test that mkdir fails with negative octal mode
        """
        o = self._cephfs_shell("mkdir -m -0755 d3")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d3 should fail
        try:
            o = self.mount_a.stat('d3')
            log.info("mount_a output:\n{}".format(o))
        except:
            pass

    def test_mkdir_with_non_octal_mode(self):
        """
        Test that mkdir passes with non-octal mode
        """
        o = self._cephfs_shell("mkdir -m u=rwx d4")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d4 should pass
        o = self.mount_a.stat('d4')
        assert((o['st_mode'] & 0o700) == 0o700)

    def test_mkdir_with_bad_non_octal_mode(self):
        """
        Test that mkdir failes with bad non-octal mode
        """
        o = self._cephfs_shell("mkdir -m ugx=0755 d5")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d5 should fail
        try:
            o = self.mount_a.stat('d5')
            log.info("mount_a output:\n{}".format(o))
        except:
            pass

    def test_mkdir_path_without_path_option(self):
        """
        Test that mkdir fails without path option for creating path
        """
        o = self._cephfs_shell("mkdir d5/d6/d7")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d5/d6/d7 should fail
        try:
            o = self.mount_a.stat('d5/d6/d7')
            log.info("mount_a output:\n{}".format(o))
        except:
            pass

    def test_mkdir_path_with_path_option(self):
        """
        Test that mkdir passes with path option for creating path
        """
        o = self._cephfs_shell("mkdir -p d5/d6/d7")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d5/d6/d7 should pass
        o = self.mount_a.stat('d5/d6/d7')
        log.info("mount_a output:\n{}".format(o))

class TestGetAndPut(TestCephFSShell):
    # the 'put' command gets tested as well with the 'get' comamnd
    def test_put_and_get_without_target_directory(self):
        """
        Test that put fails without target path
        """
        # generate test data in a directory
        self._cephfs_shell("!mkdir p1")
        self._cephfs_shell('!dd if=/dev/urandom of=p1/dump1 bs=1M count=1')
        self._cephfs_shell('!dd if=/dev/urandom of=p1/dump2 bs=2M count=1')
        self._cephfs_shell('!dd if=/dev/urandom of=p1/dump3 bs=3M count=1')

        # copy the whole directory over to the cephfs
        o = self._cephfs_shell("put p1")
        log.info("cephfs-shell output:\n{}".format(o))

        # put p1 should pass
        o = self.mount_a.stat('p1')
        log.info("mount_a output:\n{}".format(o))
        o = self.mount_a.stat('p1/dump1')
        log.info("mount_a output:\n{}".format(o))
        o = self.mount_a.stat('p1/dump2')
        log.info("mount_a output:\n{}".format(o))
        o = self.mount_a.stat('p1/dump3')
        log.info("mount_a output:\n{}".format(o))

        self._cephfs_shell('!rm -rf p1')
        o = self._cephfs_shell("get p1")
        o = self._cephfs_shell('!stat p1 || echo $?')
        log.info("cephfs-shell output:\n{}".format(o))
        self.validate_stat_output(o)

        o = self._cephfs_shell('!stat p1/dump1 || echo $?')
        log.info("cephfs-shell output:\n{}".format(o))
        self.validate_stat_output(o)

        o = self._cephfs_shell('!stat p1/dump2 || echo $?')
        log.info("cephfs-shell output:\n{}".format(o))
        self.validate_stat_output(o)

        o = self._cephfs_shell('!stat p1/dump3 || echo $?')
        log.info("cephfs-shell output:\n{}".format(o))
        self.validate_stat_output(o)

    def validate_stat_output(self, s):
        l = s.split('\n')
        log.info("lines:\n{}".format(l))
        rv = l[-1] # get last line; a failed stat will have "1" as the line
        log.info("rv:{}".format(rv))
        r = 0
        try:
            r = int(rv) # a non-numeric line will cause an exception
        except:
            pass
        assert(r == 0)

    def test_get_with_target_name(self):
        """
        Test that get passes with target name
        """
        s = 'C' * 1024
        s_hash = crypt.crypt(s, '.A')
        o = self._cephfs_shell("put - dump4", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))

        # put - dump4 should pass
        o = self.mount_a.stat('dump4')
        log.info("mount_a output:\n{}".format(o))

        o = self._cephfs_shell("get dump4 .")
        log.info("cephfs-shell output:\n{}".format(o))

        o = self._cephfs_shell("!cat dump4")
        o_hash = crypt.crypt(o, '.A')

        # s_hash must be equal to o_hash
        log.info("s_hash:{}".format(s_hash))
        log.info("o_hash:{}".format(o_hash))
        assert(s_hash == o_hash)

    def test_get_without_target_name(self):
        """
        Test that get passes with target name
        """
        s = 'D' * 1024
        o = self._cephfs_shell("put - dump5", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))

        # put - dump5 should pass
        o = self.mount_a.stat('dump5')
        log.info("mount_a output:\n{}".format(o))

        # get dump5 should fail
        o = self._cephfs_shell("get dump5")
        o = self._cephfs_shell("!stat dump5 || echo $?")
        log.info("cephfs-shell output:\n{}".format(o))
        l = o.split('\n')
        try:
            ret = int(l[1])
            # verify that stat dump5 passes
            # if ret == 1, then that implies the stat failed
            # which implies that there was a problem with "get dump5"
            assert(ret != 1)
        except ValueError:
            # we have a valid stat output; so this is good
            # if the int() fails then that means there's a valid stat output
            pass

    def test_get_to_console(self):
        """
        Test that get passes with target name
        """
        s = 'E' * 1024
        s_hash = crypt.crypt(s, '.A')
        o = self._cephfs_shell("put - dump6", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))

        # put - dump6 should pass
        o = self.mount_a.stat('dump6')
        log.info("mount_a output:\n{}".format(o))

        # get dump6 - should pass
        o = self._cephfs_shell("get dump6 -")
        o_hash = crypt.crypt(o, '.A')
        log.info("cephfs-shell output:\n{}".format(o))

        # s_hash must be equal to o_hash
        log.info("s_hash:{}".format(s_hash))
        log.info("o_hash:{}".format(o_hash))
        assert(s_hash == o_hash)

class TestCD(TestCephFSShell):
    CLIENTS_REQUIRED = 1

    def test_cd_with_no_args(self):
        """
        Test that when cd is issued without any arguments, CWD is changed
        to root directory.
        """
        path = 'dir1/dir2/dir3'
        self.mount_a.run_shell('mkdir -p ' + path)
        expected_cwd = '/'

        script = 'cd {}\ncd\ncwd\n'.format(path)
        output = self.get_cephfs_shell_script_output(script)
        self.assertEqual(output, expected_cwd)

    def test_cd_with_args(self):
        """
        Test that when cd is issued with an argument, CWD is changed
        to the path passed in the argument.
        """
        path = 'dir1/dir2/dir3'
        self.mount_a.run_shell('mkdir -p ' + path)
        expected_cwd = '/dir1/dir2/dir3'

        script = 'cd {}\ncwd\n'.format(path)
        output = self.get_cephfs_shell_script_output(script)
        self.assertEqual(output, expected_cwd)

#    def test_ls(self):
#        """
#        Test that ls passes
#        """
#        o = self._cephfs_shell("ls")
#        log.info("cephfs-shell output:\n{}".format(o))
#
#        o = self.mount_a.run_shell(['ls']).stdout.getvalue().strip().replace("\n", " ").split()
#        log.info("mount_a output:\n{}".format(o))
#
#        # ls should not list hidden files without the -a switch
#        if '.' in o or '..' in o:
#            log.info('ls failed')
#        else:
#            log.info('ls succeeded')
#
#    def test_ls_a(self):
#        """
#        Test that ls -a passes
#        """
#        o = self._cephfs_shell("ls -a")
#        log.info("cephfs-shell output:\n{}".format(o))
#
#        o = self.mount_a.run_shell(['ls', '-a']).stdout.getvalue().strip().replace("\n", " ").split()
#        log.info("mount_a output:\n{}".format(o))
#
#        if '.' in o and '..' in o:
#            log.info('ls -a succeeded')
#        else:
#            log.info('ls -a failed')

class TestMisc(TestCephFSShell):
    def test_help(self):
        """
        Test that help outputs commands.
        """

        o = self._cephfs_shell("help")

        log.info("output:\n{}".format(o))
