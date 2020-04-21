"""
Before running this testsuite, add path to cephfs-shell module to $PATH and
export $PATH.
"""
from io import StringIO
from os import path
from os import getcwd as os_getcwd
import crypt
import logging
from tempfile import mkstemp as tempfile_mkstemp
from tempfile import mkdtemp as tempfile_mkdtemp
import math
from six import ensure_str
from sys import version_info as sys_version_info
from re import search as re_search
from time import sleep
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.misc import sudo_write_file
from teuthology.misc import sh as misc_sh
from teuthology.orchestra.run import CommandFailedError

log = logging.getLogger(__name__)

def humansize(nbytes):
    suffixes = ['B', 'K', 'M', 'G', 'T', 'P']
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    nbytes = math.ceil(nbytes)
    f = ('%d' % nbytes).rstrip('.')
    return '%s%s' % (f, suffixes[i])

class TestCephFSShell(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    def run_cephfs_shell_cmd(self, cmd, mount_x=None, shell_conf_path=None,
                             opts=None, stdout=None, stderr=None, stdin=None,
                             check_status=True):
        stdout = stdout or StringIO()
        stderr = stderr or StringIO()
        if mount_x is None:
            mount_x = self.mount_a

        if isinstance(cmd, list):
            cmd = " ".join(cmd)

        args = ["cephfs-shell"]
        if shell_conf_path:
            args += ["-c", shell_conf_path]
        if opts:
            args += opts
        args.extend(("--", cmd))

        log.info("Running command: {}".format(" ".join(args)))
        return mount_x.client_remote.run(args=args, stdout=stdout,
                                         stderr=stderr, stdin=stdin,
                                         check_status=check_status)

    def negtest_cephfs_shell_cmd(self, **kwargs):
        """
        This method verifies that cephfs shell command fails with expected
        return value and/or error message.

        kwargs is expected to hold the arguments same as
        run_cephfs_shell_cmd() with the following exceptions -
            * It should not contain check_status (since commands are expected
              to fail, check_status is hardcoded to False).
            * It is optional to set expected error message and return value to
              dict members 'errmsg' and 'retval' respectively.

        This method servers as shorthand for codeblocks like -

        try:
            proc = self.run_cephfs_shell_cmd(args=['some', 'cmd'],
                                             check_status=False,
                                             stdout=stdout)
        except CommandFailedError as e:
            self.assertNotIn('some error message',
                              proc.stderr.getvalue.lower())


        try:
            proc = self.run_cephfs_shell_cmd(args=['some', 'cmd'],
                                             check_status=False,
                                             stdout=stdout)
        except CommandFailedError as e:
            self.assertNotEqual(1, proc.returncode)
        """
        retval = kwargs.pop('retval', None)
        errmsg = kwargs.pop('errmsg', None)
        kwargs['check_status'] = False

        proc = self.run_cephfs_shell_cmd(**kwargs)
        if retval:
            self.assertEqual(proc.returncode, retval)
        else:
            self.assertNotEqual(proc.returncode, 0)
        if errmsg:
            self.assertIn(errmsg, proc.stderr.getvalue().lower())

        return proc

    def get_cephfs_shell_cmd_output(self, cmd, mount_x=None,
                                    shell_conf_path=None, opts=None,
                                    stdout=None, stdin=None,check_status=True):
        return ensure_str(self.run_cephfs_shell_cmd(
            cmd=cmd, mount_x=mount_x, shell_conf_path=shell_conf_path,
            opts=opts, stdout=stdout, stdin=stdin,
            check_status=check_status).stdout.getvalue().strip())

    def get_cephfs_shell_cmd_error(self, cmd, mount_x=None,
                                   shell_conf_path=None, opts=None,
                                   stderr=None, stdin=None, check_status=True):
        return ensure_str(self.run_cephfs_shell_cmd(
            cmd=cmd, mount_x=mount_x, shell_conf_path=shell_conf_path,
            opts=opts, stderr=stderr, stdin=stdin,
            check_status=check_status).stderr.getvalue().strip())

    def run_cephfs_shell_script(self, script, mount_x=None,
                                shell_conf_path=None, opts=None, stdout=None,
                                stderr=None, stdin=None, check_status=True):
        stdout = stdout or StringIO()
        stderr = stderr or StringIO()
        if mount_x is None:
            mount_x = self.mount_a

        scriptpath = tempfile_mkstemp(prefix='test-cephfs', text=True)[1]
        with open(scriptpath, 'w') as scriptfile:
            scriptfile.write(script)
        # copy script to the machine running cephfs-shell.
        mount_x.client_remote.put_file(scriptpath, scriptpath)
        mount_x.run_shell('chmod 755 ' + scriptpath)

        args = ["cephfs-shell", '-b', scriptpath]
        if shell_conf_path:
            args[1:1] = ["-c", shell_conf_path]
        log.info('Running script \"' + scriptpath + '\"')
        return mount_x.client_remote.run(args=args, stdout=stdout,
                                         stderr=stderr, stdin=stdin,
                                         check_status=True)

    def get_cephfs_shell_script_output(self, script, mount_x=None,
                                       shell_conf_path=None, opts=None,
                                       stdout=None, stdin=None,
                                       check_status=True):
        return ensure_str(self.run_cephfs_shell_script(
            script=script, mount_x=mount_x, shell_conf_path=shell_conf_path,
            opts=opts, stdout=stdout, stdin=stdin,
            check_status=check_status).stdout.getvalue().strip())


class TestMkdir(TestCephFSShell):
    def test_mkdir(self):
        """
        Test that mkdir creates directory
        """
        o = self.get_cephfs_shell_cmd_output("mkdir d1")
        log.info("cephfs-shell output:\n{}".format(o))

        o = self.mount_a.stat('d1')
        log.info("mount_a output:\n{}".format(o))

    def test_mkdir_with_07000_octal_mode(self):
        """
        Test that mkdir fails with octal mode greater than 0777
        """
        self.negtest_cephfs_shell_cmd(cmd="mkdir -m 07000 d2")
        try:
            self.mount_a.stat('d2')
        except CommandFailedError:
            pass

    def test_mkdir_with_negative_octal_mode(self):
        """
        Test that mkdir fails with negative octal mode
        """
        self.negtest_cephfs_shell_cmd(cmd="mkdir -m -0755 d3")
        try:
            self.mount_a.stat('d3')
        except CommandFailedError:
            pass

    def test_mkdir_with_non_octal_mode(self):
        """
        Test that mkdir passes with non-octal mode
        """
        o = self.get_cephfs_shell_cmd_output("mkdir -m u=rwx d4")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d4 should pass
        o = self.mount_a.stat('d4')
        assert((o['st_mode'] & 0o700) == 0o700)

    def test_mkdir_with_bad_non_octal_mode(self):
        """
        Test that mkdir failes with bad non-octal mode
        """
        self.negtest_cephfs_shell_cmd(cmd="mkdir -m ugx=0755 d5")
        try:
            self.mount_a.stat('d5')
        except CommandFailedError:
            pass

    def test_mkdir_path_without_path_option(self):
        """
        Test that mkdir fails without path option for creating path
        """
        self.negtest_cephfs_shell_cmd(cmd="mkdir d5/d6/d7")
        try:
            self.mount_a.stat('d5/d6/d7')
        except CommandFailedError:
            pass

    def test_mkdir_path_with_path_option(self):
        """
        Test that mkdir passes with path option for creating path
        """
        o = self.get_cephfs_shell_cmd_output("mkdir -p d5/d6/d7")
        log.info("cephfs-shell output:\n{}".format(o))

        # mkdir d5/d6/d7 should pass
        o = self.mount_a.stat('d5/d6/d7')
        log.info("mount_a output:\n{}".format(o))

class TestRmdir(TestCephFSShell):
    dir_name = "test_dir"

    def dir_does_not_exists(self):
        """
        Tests that directory does not exists
        """
        try:
            self.mount_a.stat(self.dir_name)
        except CommandFailedError as e:
            if  e.exitstatus == 2:
                return 0
            raise

    def test_rmdir(self):
        """
        Test that rmdir deletes directory
        """
        self.run_cephfs_shell_cmd("mkdir " + self.dir_name)
        self.run_cephfs_shell_cmd("rmdir "+ self.dir_name)
        self.dir_does_not_exists()

    def test_rmdir_non_existing_dir(self):
        """
        Test that rmdir does not delete a non existing directory
        """
        self.negtest_cephfs_shell_cmd(cmd="rmdir test_dir")
        self.dir_does_not_exists()

    def test_rmdir_dir_with_file(self):
        """
        Test that rmdir does not delete directory containing file
        """
        self.run_cephfs_shell_cmd("mkdir " + self.dir_name)
        self.run_cephfs_shell_cmd("put - test_dir/dumpfile", stdin="Valid File")
        self.run_cephfs_shell_cmd("rmdir" + self.dir_name)
        self.mount_a.stat(self.dir_name)

    def test_rmdir_existing_file(self):
        """
        Test that rmdir does not delete a file
        """
        self.run_cephfs_shell_cmd("put - dumpfile", stdin="Valid File")
        self.negtest_cephfs_shell_cmd(cmd="rmdir dumpfile")
        self.mount_a.stat("dumpfile")

    def test_rmdir_p(self):
        """
        Test that rmdir -p deletes all empty directories in the root directory passed
        """
        self.run_cephfs_shell_cmd("mkdir -p test_dir/t1/t2/t3")
        self.run_cephfs_shell_cmd("rmdir -p "+ self.dir_name)
        self.dir_does_not_exists()

    def test_rmdir_p_valid_path(self):
        """
        Test that rmdir -p deletes all empty directories in the path passed
        """
        self.run_cephfs_shell_cmd("mkdir -p test_dir/t1/t2/t3")
        self.run_cephfs_shell_cmd("rmdir -p test_dir/t1/t2/t3")
        self.dir_does_not_exists()

    def test_rmdir_p_non_existing_dir(self):
        """
        Test that rmdir -p does not delete an invalid directory
        """
        self.negtest_cephfs_shell_cmd(cmd="rmdir -p test_dir")
        self.dir_does_not_exists()

    def test_rmdir_p_dir_with_file(self):
        """
        Test that rmdir -p does not delete the directory containing a file
        """
        self.run_cephfs_shell_cmd("mkdir " + self.dir_name)
        self.run_cephfs_shell_cmd("put - test_dir/dumpfile", stdin="Valid File")
        self.run_cephfs_shell_cmd("rmdir -p " + self.dir_name)
        self.mount_a.stat(self.dir_name)

class TestGetAndPut(TestCephFSShell):
    def test_without_target_dir(self):
        """
        Test put and get commands without target path.
        """
        tempdir = self.mount_a.client_remote.mkdtemp()
        tempdirname = path.basename(tempdir)
        files = ('dump1', 'dump2', 'dump3', tempdirname)

        for i, file_ in enumerate(files[ : -1]):
            size = i + 1
            ofarg = 'of=' + path.join(tempdir, file_)
            bsarg = 'bs=' + str(size) + 'M'
            self.mount_a.run_shell(['dd', 'if=/dev/urandom', ofarg, bsarg,
                                    'count=1'])

        self.run_cephfs_shell_cmd('put ' + tempdir)
        for file_ in files:
            if file_ == tempdirname:
                self.mount_a.stat(path.join(self.mount_a.mountpoint, file_))
            else:
                self.mount_a.stat(path.join(self.mount_a.mountpoint,
                                            tempdirname, file_))

        self.mount_a.run_shell(['rm', '-rf', tempdir])

        self.run_cephfs_shell_cmd('get ' + tempdirname)
        pwd = self.get_cephfs_shell_cmd_output('!pwd')
        for file_ in files:
            if file_ == tempdirname:
               self.mount_a.run_shell('stat ' + path.join(pwd, file_))
            else:
               self.mount_a.run_shell('stat ' + path.join(pwd, tempdirname,
                                                          file_))

    def test_get_with_target_name(self):
        """
        Test that get passes with target name
        """
        s = 'C' * 1024
        s_hash = crypt.crypt(s, '.A')
        o = self.get_cephfs_shell_cmd_output("put - dump4", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))

        # put - dump4 should pass
        o = self.mount_a.stat('dump4')
        log.info("mount_a output:\n{}".format(o))

        o = self.get_cephfs_shell_cmd_output("get dump4 .")
        log.info("cephfs-shell output:\n{}".format(o))

        o = self.get_cephfs_shell_cmd_output("!cat dump4")
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
        o = self.get_cephfs_shell_cmd_output("put - dump5", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))

        # put - dump5 should pass
        o = self.mount_a.stat('dump5')
        log.info("mount_a output:\n{}".format(o))

        # get dump5 should fail
        o = self.get_cephfs_shell_cmd_output("get dump5")
        o = self.get_cephfs_shell_cmd_output("!stat dump5 || echo $?")
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
        o = self.get_cephfs_shell_cmd_output("put - dump6", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))

        # put - dump6 should pass
        o = self.mount_a.stat('dump6')
        log.info("mount_a output:\n{}".format(o))

        # get dump6 - should pass
        o = self.get_cephfs_shell_cmd_output("get dump6 -")
        o_hash = crypt.crypt(o, '.A')
        log.info("cephfs-shell output:\n{}".format(o))

        # s_hash must be equal to o_hash
        log.info("s_hash:{}".format(s_hash))
        log.info("o_hash:{}".format(o_hash))
        assert(s_hash == o_hash)

class TestSnapshots(TestCephFSShell):
    def test_snap(self):
        """
        Test that snapshot creation and deletion work
        """
        sd = self.fs.get_config('client_snapdir')
        sdn = "data_dir/{}/snap1".format(sd)

        # create a data dir and dump some files into it
        self.get_cephfs_shell_cmd_output("mkdir data_dir")
        s = 'A' * 10240
        o = self.get_cephfs_shell_cmd_output("put - data_dir/data_a", stdin=s)
        s = 'B' * 10240
        o = self.get_cephfs_shell_cmd_output("put - data_dir/data_b", stdin=s)
        s = 'C' * 10240
        o = self.get_cephfs_shell_cmd_output("put - data_dir/data_c", stdin=s)
        s = 'D' * 10240
        o = self.get_cephfs_shell_cmd_output("put - data_dir/data_d", stdin=s)
        s = 'E' * 10240
        o = self.get_cephfs_shell_cmd_output("put - data_dir/data_e", stdin=s)

        o = self.get_cephfs_shell_cmd_output("ls -l /data_dir")
        log.info("cephfs-shell output:\n{}".format(o))

        # create the snapshot - must pass
        o = self.get_cephfs_shell_cmd_output("snap create snap1 /data_dir")
        log.info("cephfs-shell output:\n{}".format(o))
        self.assertEqual("", o)
        o = self.mount_a.stat(sdn)
        log.info("mount_a output:\n{}".format(o))
        self.assertIn('st_mode', o)

        # create the same snapshot again - must fail with an error message
        self.negtest_cephfs_shell_cmd(cmd="snap create snap1 /data_dir",
                                      errmsg="snapshot 'snap1' already exists")
        o = self.mount_a.stat(sdn)
        log.info("mount_a output:\n{}".format(o))
        self.assertIn('st_mode', o)

        # delete the snapshot - must pass
        o = self.get_cephfs_shell_cmd_output("snap delete snap1 /data_dir")
        log.info("cephfs-shell output:\n{}".format(o))
        self.assertEqual("", o)
        try:
            o = self.mount_a.stat(sdn)
        except CommandFailedError:
            # snap dir should not exist anymore
            pass
        log.info("mount_a output:\n{}".format(o))
        self.assertNotIn('st_mode', o)

        # delete the same snapshot again - must fail with an error message
        self.negtest_cephfs_shell_cmd(cmd="snap delete snap1 /data_dir",
                                      errmsg="'snap1': no such snapshot")
        try:
            o = self.mount_a.stat(sdn)
        except CommandFailedError:
            pass
        log.info("mount_a output:\n{}".format(o))
        self.assertNotIn('st_mode', o)

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

class TestDU(TestCephFSShell):
    CLIENTS_REQUIRED = 1

    def test_du_works_for_regfiles(self):
        regfilename = 'some_regfile'
        regfile_abspath = path.join(self.mount_a.mountpoint, regfilename)
        sudo_write_file(self.mount_a.client_remote, regfile_abspath, 'somedata')

        size = humansize(self.mount_a.stat(regfile_abspath)['st_size'])
        expected_output = r'{}{}{}'.format(size, " +", regfilename)

        du_output = self.get_cephfs_shell_cmd_output('du ' + regfilename)
        if sys_version_info.major >= 3:
            self.assertRegex(du_output, expected_output)
        elif sys_version_info.major < 3:
            assert re_search(expected_output, du_output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   expected_output, du_output)

    def test_du_works_for_non_empty_dirs(self):
        dirname = 'some_directory'
        dir_abspath = path.join(self.mount_a.mountpoint, dirname)
        regfilename = 'some_regfile'
        regfile_abspath = path.join(dir_abspath, regfilename)
        self.mount_a.run_shell('mkdir ' + dir_abspath)
        sudo_write_file(self.mount_a.client_remote, regfile_abspath, 'somedata')

        # XXX: we stat `regfile_abspath` here because ceph du reports a non-empty
        # directory's size as sum of sizes of all files under it.
        size = humansize(self.mount_a.stat(regfile_abspath)['st_size'])
        expected_output = r'{}{}{}'.format(size, " +", dirname)

        sleep(10)
        du_output = self.get_cephfs_shell_cmd_output('du ' + dirname)
        if sys_version_info.major >= 3:
            self.assertRegex(du_output, expected_output)
        elif sys_version_info.major < 3:
            assert re_search(expected_output, du_output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   expected_output, du_output)

    def test_du_works_for_empty_dirs(self):
        dirname = 'some_directory'
        dir_abspath = path.join(self.mount_a.mountpoint, dirname)
        self.mount_a.run_shell('mkdir ' + dir_abspath)

        size = humansize(self.mount_a.stat(dir_abspath)['st_size'])
        expected_output = r'{}{}{}'.format(size, " +", dirname)

        du_output = self.get_cephfs_shell_cmd_output('du ' + dirname)
        if sys_version_info.major >= 3:
            self.assertRegex(du_output, expected_output)
        elif sys_version_info.major < 3:
            assert re_search(expected_output, du_output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   expected_output, du_output)

    def test_du_works_for_hardlinks(self):
        regfilename = 'some_regfile'
        regfile_abspath = path.join(self.mount_a.mountpoint, regfilename)
        sudo_write_file(self.mount_a.client_remote, regfile_abspath,
                        'somedata')
        hlinkname = 'some_hardlink'
        hlink_abspath = path.join(self.mount_a.mountpoint, hlinkname)
        self.mount_a.run_shell(['sudo', 'ln', regfile_abspath,
                                hlink_abspath], omit_sudo=False)

        size = humansize(self.mount_a.stat(hlink_abspath)['st_size'])
        expected_output = r'{}{}{}'.format(size, " +", hlinkname)

        du_output = self.get_cephfs_shell_cmd_output('du ' + hlinkname)
        if sys_version_info.major >= 3:
            self.assertRegex(du_output, expected_output)
        elif sys_version_info.major < 3:
            assert re_search(expected_output, du_output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   expected_output, du_output)

    def test_du_works_for_softlinks_to_files(self):
        regfilename = 'some_regfile'
        regfile_abspath = path.join(self.mount_a.mountpoint, regfilename)
        sudo_write_file(self.mount_a.client_remote, regfile_abspath, 'somedata')
        slinkname = 'some_softlink'
        slink_abspath = path.join(self.mount_a.mountpoint, slinkname)
        self.mount_a.run_shell(['ln', '-s', regfile_abspath, slink_abspath])

        size = humansize(self.mount_a.lstat(slink_abspath)['st_size'])
        expected_output = r'{}{}{}'.format((size), " +", slinkname)

        du_output = self.get_cephfs_shell_cmd_output('du ' + slinkname)
        if sys_version_info.major >= 3:
            self.assertRegex(du_output, expected_output)
        elif sys_version_info.major < 3:
            assert re_search(expected_output, du_output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   expected_output, du_output)

    def test_du_works_for_softlinks_to_dirs(self):
        dirname = 'some_directory'
        dir_abspath = path.join(self.mount_a.mountpoint, dirname)
        self.mount_a.run_shell('mkdir ' + dir_abspath)
        slinkname = 'some_softlink'
        slink_abspath = path.join(self.mount_a.mountpoint, slinkname)
        self.mount_a.run_shell(['ln', '-s', dir_abspath, slink_abspath])

        size = humansize(self.mount_a.lstat(slink_abspath)['st_size'])
        expected_output = r'{}{}{}'.format(size, " +", slinkname)

        du_output = self.get_cephfs_shell_cmd_output('du ' + slinkname)
        if sys_version_info.major >= 3:
            self.assertRegex(du_output, expected_output)
        elif sys_version_info.major < 3:
            assert re_search(expected_output, du_output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   expected_output, du_output)

    # NOTE: tests using these are pretty slow since to this methods sleeps for
    # 15 seconds
    def _setup_files(self, return_path_to_files=False, path_prefix='./'):
        dirname = 'dir1'
        regfilename = 'regfile'
        hlinkname = 'hlink'
        slinkname = 'slink1'
        slink2name = 'slink2'

        dir_abspath = path.join(self.mount_a.mountpoint, dirname)
        regfile_abspath = path.join(self.mount_a.mountpoint, regfilename)
        hlink_abspath = path.join(self.mount_a.mountpoint, hlinkname)
        slink_abspath = path.join(self.mount_a.mountpoint, slinkname)
        slink2_abspath = path.join(self.mount_a.mountpoint, slink2name)

        self.mount_a.run_shell('mkdir ' + dir_abspath)
        self.mount_a.run_shell('touch ' + regfile_abspath)
        self.mount_a.run_shell(['ln', regfile_abspath, hlink_abspath])
        self.mount_a.run_shell(['ln', '-s', regfile_abspath, slink_abspath])
        self.mount_a.run_shell(['ln', '-s', dir_abspath, slink2_abspath])

        dir2_name = 'dir2'
        dir21_name = 'dir21'
        regfile121_name = 'regfile121'
        dir2_abspath = path.join(self.mount_a.mountpoint, dir2_name)
        dir21_abspath = path.join(dir2_abspath, dir21_name)
        regfile121_abspath = path.join(dir21_abspath, regfile121_name)
        self.mount_a.run_shell('mkdir -p ' + dir21_abspath)
        self.mount_a.run_shell('touch ' + regfile121_abspath)

        sudo_write_file(self.mount_a.client_remote, regfile_abspath,
            'somedata')
        sudo_write_file(self.mount_a.client_remote, regfile121_abspath,
            'somemoredata')

        # TODO: is there a way to trigger/force update ceph.dir.rbytes?
        # wait so that attr ceph.dir.rbytes gets a chance to be updated.
        sleep(20)

        expected_patterns = []
        path_to_files = []

        def append_expected_output_pattern(f):
            if f == '/':
                expected_patterns.append(r'{}{}{}'.format(size, " +", '.' + f))
            else:
                expected_patterns.append(r'{}{}{}'.format(size, " +",
                    path_prefix + path.relpath(f, self.mount_a.mountpoint)))

        for f in [dir_abspath, regfile_abspath, regfile121_abspath,
                  hlink_abspath, slink_abspath, slink2_abspath]:
            size = humansize(self.mount_a.stat(f, follow_symlinks=
                                               False)['st_size'])
            append_expected_output_pattern(f)

        # get size for directories containig regfiles within
        for f in [dir2_abspath, dir21_abspath]:
            size = humansize(self.mount_a.stat(regfile121_abspath,
                             follow_symlinks=False)['st_size'])
            append_expected_output_pattern(f)

        # get size for CephFS root
        size = 0
        for f in [regfile_abspath, regfile121_abspath, slink_abspath,
                  slink2_abspath]:
            size += self.mount_a.stat(f, follow_symlinks=False)['st_size']
        size = humansize(size)
        append_expected_output_pattern('/')

        if return_path_to_files:
            for p in [dir_abspath, regfile_abspath, dir2_abspath,
                      dir21_abspath, regfile121_abspath, hlink_abspath,
                      slink_abspath, slink2_abspath]:
                 path_to_files.append(path.relpath(p, self.mount_a.mountpoint))

            return (expected_patterns, path_to_files)
        else:
            return expected_patterns

    def test_du_works_recursively_with_no_path_in_args(self):
        expected_patterns_in_output = self._setup_files()
        du_output = self.get_cephfs_shell_cmd_output('du -r')

        for expected_output in expected_patterns_in_output:
            if sys_version_info.major >= 3:
                self.assertRegex(du_output, expected_output)
            elif sys_version_info.major < 3:
                assert re_search(expected_output, du_output) != None, "\n" + \
                       "expected_output -\n{}\ndu_output -\n{}\n".format(
                       expected_output, du_output)

    def test_du_with_path_in_args(self):
        expected_patterns_in_output, path_to_files = self._setup_files(True,
            path_prefix='')

        args = ['du', '/']
        for p in path_to_files:
            args.append(p)
        du_output = self.get_cephfs_shell_cmd_output(args)

        for expected_output in expected_patterns_in_output:
            if sys_version_info.major >= 3:
                self.assertRegex(du_output, expected_output)
            elif sys_version_info.major < 3:
                assert re_search(expected_output, du_output) != None, "\n" +\
                       "expected_output -\n{}\ndu_output -\n{}\n".format(
                       expected_output, du_output)

    def test_du_with_no_args(self):
        expected_patterns_in_output = self._setup_files()

        du_output = self.get_cephfs_shell_cmd_output('du')

        for expected_output in expected_patterns_in_output:
            # Since CWD is CephFS root and being non-recursive expect only
            # CWD in DU report.
            if expected_output.find('/') == len(expected_output) - 1:
                if sys_version_info.major >= 3:
                    self.assertRegex(du_output, expected_output)
                elif sys_version_info.major < 3:
                    assert re_search(expected_output, du_output) != None, "\n" + \
                        "expected_output -\n{}\ndu_output -\n{}\n".format(
                        expected_output, du_output)


class TestDF(TestCephFSShell):
    def validate_df(self, filename):
        df_output = self.get_cephfs_shell_cmd_output('df '+filename)
        log.info("cephfs-shell df output:\n{}".format(df_output))

        shell_df = df_output.splitlines()[1].split()

        block_size = int(self.mount_a.df()["total"]) // 1024
        log.info("cephfs df block size output:{}\n".format(block_size))

        st_size = int(self.mount_a.stat(filename)["st_size"])
        log.info("cephfs stat used output:{}".format(st_size))
        log.info("cephfs available:{}\n".format(block_size - st_size))

        self.assertTupleEqual((block_size, st_size, block_size - st_size),
            (int(shell_df[0]), int(shell_df[1]) , int(shell_df[2])))

    def test_df_with_no_args(self):
        expected_output = ''
        df_output = self.get_cephfs_shell_cmd_output('df')
        assert df_output == expected_output

    def test_df_for_valid_directory(self):
        dir_name = 'dir1'
        mount_output = self.mount_a.run_shell('mkdir ' + dir_name)
        log.info("cephfs-shell mount output:\n{}".format(mount_output))
        self.validate_df(dir_name)

    def test_df_for_invalid_directory(self):
        dir_abspath = path.join(self.mount_a.mountpoint, 'non-existent-dir')
        self.negtest_cephfs_shell_cmd(cmd='df ' + dir_abspath,
                                      errmsg='error in stat')

    def test_df_for_valid_file(self):
        s = 'df test' * 14145016
        o = self.get_cephfs_shell_cmd_output("put - dumpfile", stdin=s)
        log.info("cephfs-shell output:\n{}".format(o))
        self.validate_df("dumpfile")


class TestQuota(TestCephFSShell):
    dir_name = 'testdir'

    def create_dir(self):
        mount_output = self.get_cephfs_shell_cmd_output('mkdir ' + self.dir_name)
        log.info("cephfs-shell mount output:\n{}".format(mount_output))

    def set_and_get_quota_vals(self, input_val, check_status=True):
        self.run_cephfs_shell_cmd(['quota', 'set', '--max_bytes',
                                   input_val[0], '--max_files', input_val[1],
                                   self.dir_name], check_status=check_status)

        quota_output = self.get_cephfs_shell_cmd_output(['quota', 'get', self.dir_name],
                                                        check_status=check_status)

        quota_output = quota_output.split()
        return quota_output[1], quota_output[3]

    def test_set(self):
        self.create_dir()
        set_values = ('6', '2')
        self.assertTupleEqual(self.set_and_get_quota_vals(set_values), set_values)

    def test_replace_values(self):
        self.test_set()
        set_values = ('20', '4')
        self.assertTupleEqual(self.set_and_get_quota_vals(set_values), set_values)

    def test_set_invalid_dir(self):
        set_values = ('5', '5')
        try:
            self.assertTupleEqual(self.set_and_get_quota_vals(
                                  set_values, False), set_values)
            raise Exception("Something went wrong!! Values set for non existing directory")
        except IndexError:
            # Test should pass as values cannot be set for non existing directory
            pass

    def test_set_invalid_values(self):
        self.create_dir()
        set_values = ('-6', '-5')
        try:
            self.assertTupleEqual(self.set_and_get_quota_vals(set_values,
                                  False), set_values)
            raise Exception("Something went wrong!! Invalid values set")
        except IndexError:
            # Test should pass as invalid values cannot be set
            pass

    def test_exceed_file_limit(self):
        self.test_set()
        dir_abspath = path.join(self.mount_a.mountpoint, self.dir_name)
        self.mount_a.run_shell('touch '+dir_abspath+'/file1')
        file2 = path.join(dir_abspath, "file2")
        try:
            self.mount_a.run_shell('touch '+file2)
            raise Exception("Something went wrong!! File creation should have failed")
        except CommandFailedError:
            # Test should pass as file quota set to 2
            # Additional condition to confirm file creation failure
            if not path.exists(file2):
                return 0
            raise

    def test_exceed_write_limit(self):
        self.test_set()
        dir_abspath = path.join(self.mount_a.mountpoint, self.dir_name)
        filename = 'test_file'
        file_abspath = path.join(dir_abspath, filename)
        try:
            # Write should fail as bytes quota is set to 6
            sudo_write_file(self.mount_a.client_remote, file_abspath,
                    'Disk raise Exception')
            raise Exception("Write should have failed")
        except CommandFailedError:
            # Test should pass only when write command fails
            path_exists = path.exists(file_abspath)
            if not path_exists:
                # Testing with teuthology: No file is created.
                return 0
            elif path_exists and not path.getsize(file_abspath):
                # Testing on Fedora 30: When write fails, empty file gets created.
                return 0
            else:
                raise


class TestXattr(TestCephFSShell):
    dir_name = 'testdir'

    def create_dir(self):
        self.run_cephfs_shell_cmd('mkdir ' + self.dir_name)

    def set_get_list_xattr_vals(self, input_val, negtest=False):
        setxattr_output = self.get_cephfs_shell_cmd_output(
            ['setxattr', self.dir_name, input_val[0], input_val[1]])
        log.info("cephfs-shell setxattr output:\n{}".format(setxattr_output))

        getxattr_output = self.get_cephfs_shell_cmd_output(
            ['getxattr', self.dir_name, input_val[0]])
        log.info("cephfs-shell getxattr output:\n{}".format(getxattr_output))

        listxattr_output = self.get_cephfs_shell_cmd_output(
            ['listxattr', self.dir_name])
        log.info("cephfs-shell listxattr output:\n{}".format(listxattr_output))

        return listxattr_output, getxattr_output

    def test_set(self):
        self.create_dir()
        set_values = ('user.key', '2')
        self.assertTupleEqual(self.set_get_list_xattr_vals(set_values), set_values)

    def test_reset(self):
        self.test_set()
        set_values = ('user.key', '4')
        self.assertTupleEqual(self.set_get_list_xattr_vals(set_values), set_values)

    def test_non_existing_dir(self):
        input_val = ('user.key', '9')
        self.negtest_cephfs_shell_cmd(cmd=['setxattr', self.dir_name, input_val[0],
                                       input_val[1]])
        self.negtest_cephfs_shell_cmd(cmd=['getxattr', self.dir_name, input_val[0]])
        self.negtest_cephfs_shell_cmd(cmd=['listxattr', self.dir_name])

#    def test_ls(self):
#        """
#        Test that ls passes
#        """
#        o = self.get_cephfs_shell_cmd_output("ls")
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
#        o = self.get_cephfs_shell_cmd_output("ls -a")
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
    def test_issue_cephfs_shell_cmd_at_invocation(self):
        """
        Test that `cephfs-shell -c conf cmd` works.
        """
        # choosing a long name since short ones have a higher probability
        # of getting matched by coincidence.
        dirname = 'somedirectory'
        self.run_cephfs_shell_cmd(['mkdir', dirname])

        output = self.mount_a.client_remote.sh(['cephfs-shell', 'ls']).\
            strip()

        if sys_version_info.major >= 3:
            self.assertRegex(output, dirname)
        elif sys_version_info.major < 3:
            assert re_search(dirname, output) != None, "\n" + \
                   "expected_output -\n{}\ndu_output -\n{}\n".format(
                   dirname, output)

    def _test_every_shell_cmd_at_invocation(self):
        """
        Runs every test in this suite with
        ``cephfs-shell -c ceph.conf <cephfs-shell-command>``
        instead of ``cephfs-shell -c ceph.conf -- <cephfs-shell-command>``
        """
        from sys import stdout as sys_stdout
        from sys import modules as sys_modules
        from inspect import isclass as inspect_isclass
        from inspect import ismethod as inspect_ismethod
        from inspect import getmembers as inspect_getmembers
        from traceback import print_exc as traceback_print_exc

        def fake_run_cephfs_shell_cmd(cmd, mount_x=None, opts=None,
                                      config_path=None, stdin=None,
                                      check_status=True):
            if not mount_x:
                mount_x = self.mount_a
            if not config_path:
                config_path = self.mount_a.config_path

            cmd = " ".join(cmd) if isinstance(cmd, list) else cmd

            args = ["cephfs-shell", "-c", self.mount_a.config_path]

            if opts is not None:
                args.extend(opts)

            args.append(cmd)

            log.info("Running command: {}".format(" ".join(args)))
            return mount_x.client_remote.run(args=args, stdout=StringIO(),
                                             stderr=StringIO(), stdin=stdin,
                                             check_status=check_status)

        def fake_get_cephfs_shell_cmd_output(cmd, mount_x=None, opts=None,
                                             stdin=None, config_path=None,
                                             check_status=True):
            return ensure_str(fake_run_cephfs_shell_cmd(
                cmd=cmd, mount_x=mount_x, opts=opts, stdin=stdin,
                config_path=config_path, check_status=check_status).\
                stdout.getvalue().strip())

        def fake_get_cephfs_shell_cmd_error(cmd, mount_x=None, opts=None,
                                            stdin=None, config_path=None,
                                            check_status=True):
            return ensure_str(fake_run_cephfs_shell_cmd(
                cmd=cmd, mount_x=mount_x, opts=opts, stdin=stdin,
                config_path=config_path, check_status=check_status).\
                stderr.getvalue().strip())

        def fetch_classes_and_run_tests():
            unreqd_classes = ['CephFSTestCase', 'TestCephFSShell', 'TestMisc',
                              'StringIO']
            for class_tuple in inspect_getmembers(sys_modules[__name__],
                                                  inspect_isclass):
                if class_tuple[0] in unreqd_classes:
                    continue

                # NOTE: for debugging
                #if class_tuple[0] != 'TestGetAndPut':
                #    continue

                log.info('Running tests from class ' + class_tuple[0] + ' '+\
                         'now...')
                if not fetch_and_run_tests(class_tuple):
                    return False
                log.info('Completed running ' + class_tuple[0])

            return True

        def fetch_and_run_tests(class_tuple):
            test_count, tests_execd_count = (0, 0)

            for method_tuple in inspect_getmembers(class_tuple[1],
                                                   inspect_ismethod):
                if method_tuple[0].find('test_') != 0:
                    continue

                # NOTE: for debugging
                #if method_tuple[0] != 'test_get_to_console':
                #    continue

                test_count += 1
                obj = class_tuple[1](methodName=method_tuple[0])
                # XXX: with out this, this test is useless!
                obj.run_cephfs_shell_cmd = fake_run_cephfs_shell_cmd
                obj.get_cephfs_shell_cmd_output = \
                    fake_get_cephfs_shell_cmd_output
                obj.get_cephfs_shell_cmd_error = \
                    fake_get_cephfs_shell_cmd_error

                obj.mount_a = self.mount_a
                obj.mount_b = self.mount_b
                obj.fs = self.fs
                obj.mounts = self.mounts
                obj.mds_cluster = self.mds_cluster
                obj.mgr_cluster = self.mgr_cluster
                obj.mon_manager = self.mon_manager

                # execute test method.
                log.info('Running test ' + method_tuple[0] + 'now...')
                try:
                    self.setUp()
                    # this line runs the test method.
                    self.assertIsNot(method_tuple[1](obj), False)
                    self.tearDown()
                except AssertionError as e:
                    print('\n' + '=' * 78)
                    print('Error occurred in test %s.%s' % (class_tuple,
                          method_tuple))
                    traceback_print_exc(file=sys_stdout)
                    print('=' * 78 + '\n')
                    return False

                log.info('Completed running test ' + method_tuple[0])
                tests_execd_count += 1

            return test_count == tests_execd_count

        # NOTE: this method begins executing here.
        self.assertTrue(fetch_classes_and_run_tests())
        log.info('=' * 76)
        log.info('test_every_shell_cmd_at_invocation was successful')
        log.info('=' * 76)

    def test_help(self):
        """
        Test that help outputs commands.
        """
        o = self.get_cephfs_shell_cmd_output("help all")
        log.info("output:\n{}".format(o))

class TestShellOpts(TestCephFSShell):
    """
    Contains tests for shell options from conf file and shell prompt.
    """

    def setUp(self):
        super(type(self), self).setUp()

        # output of following command -
        # editor - was: 'vim'
        # now: '?'
        # editor: '?'
        self.editor_val = self.get_cephfs_shell_cmd_output(
            'set editor ?, set editor').split('\n')[2]
        self.editor_val = self.editor_val.split(':')[1].\
            replace("'", "", 2).strip()

    def write_tempconf(self, confcontents):
        self.tempconfpath = self.mount_a.client_remote.mktemp(
            suffix='cephfs-shell.conf')
        sudo_write_file(self.mount_a.client_remote, self.tempconfpath,
                         confcontents)

    def test_reading_conf(self):
        self.write_tempconf("[cephfs-shell]\neditor =  ???")

        # output of following command -
        # CephFS:~/>>> set editor
        # editor: 'vim'
        final_editor_val = self.get_cephfs_shell_cmd_output(
            cmd='set editor', shell_conf_path=self.tempconfpath)
        final_editor_val = final_editor_val.split(': ')[1]
        final_editor_val = final_editor_val.replace("'", "", 2)

        self.assertNotEqual(self.editor_val, final_editor_val)

    def test_reading_conf_with_dup_opt(self):
        """
        Read conf without duplicate sections/options.
        """
        self.write_tempconf("[cephfs-shell]\neditor = ???\neditor = " +
                            self.editor_val)

        # output of following command -
        # CephFS:~/>>> set editor
        # editor: 'vim'
        final_editor_val = self.get_cephfs_shell_cmd_output(
            cmd='set editor', shell_conf_path=self.tempconfpath)
        final_editor_val = final_editor_val.split(': ')[1]
        final_editor_val = final_editor_val.replace("'", "", 2)

        self.assertEqual(self.editor_val, final_editor_val)

    def test_setting_opt_after_reading_conf(self):
        self.write_tempconf("[cephfs-shell]\neditor = ???")

        # output of following command -
        # editor - was: vim
        # now: vim
        # editor: vim
        final_editor_val = self.get_cephfs_shell_cmd_output(
            cmd='set editor %s, set editor' % (self.editor_val),
            shell_conf_path=self.tempconfpath)
        final_editor_val = final_editor_val.split('\n')[2]
        final_editor_val = final_editor_val.split(': ')[1]
        final_editor_val = final_editor_val.replace("'", "", 2)

        self.assertEqual(self.editor_val, final_editor_val)
