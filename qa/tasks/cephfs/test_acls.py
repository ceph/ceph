import os
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

#
# In general this is how testcases in this file look like -
#
# =========== Setup Part ===========
#
# 1. Decide ACL to test (and ACL expression required to remove it).
# 2. Create directories as root (and as current user in some cases).
# 3. Have appropriate permissions on test directories.
#
# =========== Test appyling the ACL ===========
#
# 4. Apply ACL.
# 5. Verify its presence.
# 6. Verify its effect on FS.
#
# =========== Test removing the ACL ===========
#
# 7. Remove ACL.
# 8. Verify its absence
# 9. Verify the effect on FS.
#

class TestACLs(CephFSTestCase):
    CLIENT_REQUIED = 1
    py_version = 'python'
    # TODO: add option for kernel client
    USE_FUSE = True
    otheruser = othergroupname = otheruserpwd = 'dummy'
    __setup_once_executed_once = False

    #def __del__(self):
    #    self.mount_a.run_as_root('sudo userdel ' + self.otheruser)

    # TODO: use __init__() instead.
    def setup_once(self):
        if self.__setup_once_executed_once:
            return

        output = self.mount_a.run_shell(['id']).stdout.getvalue()
        self.currentuser = output[output.find('(') + 1 : output.find(')')]
        # for group name we need to get the characters between'(' and ')'
        # located after gid=xxxx, where xxxx is numeric gid.
        self.currentgroup = output[output.find('(', output.find('gid')) + 1 :
                                   output.find(')', output.find('gid'))]
        self.currentuser2 = 'rishabh2'

        # nondefault, nonmask ACLs
        self.fo_acl = 'user::rwx'
        self.rm_fo_acl = 'user::---'
        self.fg_acl = 'group::rwx'
        self.rm_fg_acl = 'group::---'
        self.o_acl = 'other::rwx'
        self.rm_o_acl = 'other::---'

        # ACLS for other users
        self.u_acl = "user:" + self.currentuser + ":rwx"
        self.rm_u_acl = "user:" + self.currentuser + ":"
        self.g_acl = "group:" + self.currentgroup + ":rwx"
        self.rm_g_acl = "group:" + self.currentgroup + ":"
        # nondefault, mask ACLs
        self.m_acl = 'mask::---'
        self.rm_m_acl = 'mask::'
        # default, nonmask ACLs
        self.du_acl = 'default:user:'  + self.currentuser + ':rwx'
        self.rm_du_acl  = 'default:user:'  + self.currentuser + ':'
        self.dg_acl = 'default:group:' + self.currentgroup + ':rwx'
        self.rm_dg_acl = 'default:group:' + self.currentgroup + ':'

        self.dfo_acl = 'default:user::rwx'
        self.rm_dfo_acl = 'default:user::---'
        self.dfg_acl = 'default:group::rwx'
        self.rm_dfg_acl = 'default:group::---'
        self.do_acl = 'default:other::rwx'
        self.rm_do_acl = 'default:other::---'
        # default, mask ACLs
        self.dm_acl = 'default:mask::---'
        self.rm_dm_acl =  'default:mask::'

        self.all_acls = [self.fo_acl, self.fg_acl, self.o_acl, self.u_acl,
                        self.g_acl, self.m_acl, self.du_acl, self.dg_acl,
                        self.dfo_acl, self.dfg_acl, self.do_acl, self.dm_acl]
        self.all_rm_acls = [self.rm_fo_acl, self.rm_fg_acl, self.rm_o_acl,
                            self.rm_u_acl, self.rm_g_acl, self.rm_m_acl,
                            self.rm_du_acl, self.rm_dg_acl, self.rm_dfo_acl,
                            self.rm_dfg_acl, self.rm_do_acl, self.rm_dm_acl]
        self.default_acls = [acl for acl in self.all_acls if acl.find('default') != -1]
        self.nondefault_acls = [acl for acl in self.all_acls if acl.find('default') == -1]
        self.nonremovable_acls = [acl for acl in self.all_acls if
                                  acl.find(self.currentuser) == -1 and
                                  acl.find(self.currentgroup) == -1 and
                                  acl.find('mask') == -1 and acl.find('default') == -1]
        self.removable_acls = [acl for acl in self.all_acls if
                               acl.find(self.currentuser) != -1 or
                               acl.find(self.currentgroup) != -1 or
                               acl.find('mask') != -1]

        self.testdir1 =  os.path.join(self.mount_a.mountpoint, "dir1")
        self.testdir2 = os.path.join(self.testdir1, 'dir2')
        self.testdir3 = os.path.join(self.testdir2, 'dir3')
        self.testdirs = [self.testdir1, self.testdir2, self.testdir3]

        self.__setup_once_executed_once = True

        # TODO: test without passwordless sudo
        # self.mount_a.run_as_root(['useradd', '-s', '/bin/bash', self.otheruser])
        # self.mount_a.run_shell(['sudo', 'passwd', self.otheruser, '--stdin'], stdin=self.otheruserpwd)

    def setup(self, mkdir=True, change_perms=True, change_grp_perms=True):
        self.setup_once()
        self.fs.set_ceph_conf('client', 'fuse_default_permissions', False)
        self.fs.set_ceph_conf('client', 'client_acl_type', 'posix_acl')
        # XXX: above options are effective only for new mounts, so remounting
        # mount a.
        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        mntpts_parent_dir = os.path.dirname(self.mount_a.mountpoint)
        self.mount_a.run_shell('chmod g+rwx ' + mntpts_parent_dir)
        self.mount_a.run_shell('chmod o+rwx ' + mntpts_parent_dir)

        if mkdir:
            self.create_test_dirs(change_perms=change_perms,
                                  change_grp_perms=change_grp_perms)

    def is_fo_acl(self, acl):
        return True if acl.find('user::') != -1 else False

    def is_fg_acl(self, acl):
        return True if acl.find('group::') != -1 else False

    def is_o_acl(self, acl):
        return True if acl.find('other::') != -1 else False

    def is_d_acl(self, acl):
        return True if acl.find('default:') != -1 else False

    def is_m_acl(self, acl):
        return True if acl.find('default:') != -1 else False

    def get_corresponding_rm_acl(self, acl):
        if acl.find('default') == -1 and (acl.find(self.currentuser) != -1 or \
           acl.find(self.currentgroup) != -1 or acl.find('mask') != -1):
            return acl[ : acl.find(':', acl.find(':') + 1) + 1]
        elif acl.find('default') != -1:
            return acl[ : acl.find(':', acl.find(':', acl.find(':') + 1) + 1) + 1]
        else:
            return None

    def create_test_dirs(self, perm_bits=None, as_currentuser=False,
                         change_perms=True, change_grp_perms=True):
        if perm_bits is None:
            cmd = 'mkdir --parents %s' % (self.testdir3)
        else:
            cmd = 'mkdir --parents --mode=%s %s' % (perm_bits, self.testdir3)

        if as_currentuser:
            self.mount_a.run_shell(cmd)
        else:
            self.mount_a.run_as_root(cmd)

        if change_perms:
            if change_grp_perms:
                self.mount_a.run_as_root('chmod --recursive g-rwx ' + self.testdir1)
            self.mount_a.run_as_root('chmod --recursive o-rwx ' + self.testdir1)

    def return_list(self, var):
        if str(type(var)).find('list') != -1:
            return var
        elif str(type(var)).find('str') != -1:
            return [var]
        else:
            raise RuntimeError('var "%s" (of type %s) should be either list or str.' % (var, str(type(var))))

    # assert whether acls are present or absent.
    def assert_acls_are(self, acls, testdirs, acl_present):
        if acl_present not in [True, False]:
            raise RuntimeError('acl_present should be a Boolean variable')

        acls = self.return_list(acls)
        testdirs = self.return_list(testdirs)

        for d in testdirs:
            output = self.mount_a.run_as_root('getfacl ' + d).stdout.getvalue()
            for acl in acls:
                try:
                    if acl_present:
                        assert output.find(acl) != -1
                    else:
                        assert output.find(acl) == -1
                except AssertionError as ae:
                    # make sure nondefault ACLs don't match default
                    # ACLs (e.g. "user:rishabh:rwx" should not match
                    # "default:user:rishabh:rwx")
                    if output.find(':' + acl) != -1:
                        if output.find(acl, output.find(':' + acl) + 2) == -1:
                            return

                    print('========================================================')
                    print('Error -')
                    print('ACL "%s" was expected to be %s but it is not.' % (acl, acl_present))
                    print('ACLs present -\n' + output)
                    print('========================================================')
                    raise AssertionError

    def assert_acls_are_present(self, acls, testdirs):
        self.assert_acls_are(acls, testdirs, acl_present=True)

    def assert_acls_are_absent(self, acls, testdirs):
        self.assert_acls_are(acls, testdirs, acl_present=False)

    def assert_access_is_as_expected(self, proc, expecting_access):
        try:
            if expecting_access:
                assert proc.returncode == 0
            else:
                assert proc.returncode != 0
                assert proc.stderr.getvalue().lower().find('permission denied') != -1
        except:
            print('========================================================')
            print('ERROR -')
            temp = 'expected' if expecting_access else 'not expected'
            print('access was ' + temp + ' but it isn\'t so.')
            print('proc.returncode = %s' % proc.returncode)
            if proc.returncode != 0:
                print('proc.stderr.getvalue() -\n' + proc.stderr.getvalue())
            print('========================================================')
            print('Stacktrace -')
            raise AssertionError

    # assert whether dirs are accessible or inaccessible.
    def assert_dirs_are(self, testdirs, test_as_current_user=True,
                        other_user=None, expecting_access=None):
        if str(type(expecting_access)).find('bool') == -1:
            raise RuntimeError('expecting_access should be a Boolean '
                               'variable')
        testdirs = self.return_list(testdirs)

        for d in testdirs:
            file_inside_d = os.path.join(self.mount_a.mountpoint, d,
                                         'testfile')
            for testcmd in [['ls', d], ['touch', file_inside_d],
                            ['ls', file_inside_d]]:
                if test_as_current_user:
                    proc = self.mount_a.testcmd(testcmd)
                    self.assert_access_is_as_expected(proc, expecting_access)

                if other_user is not None:
                    proc=self.mount_a.testcmd_as_user(testcmd,user=other_user)
                    self.assert_access_is_as_expected(proc, expecting_access)

    def assert_dirs_are_accessible(self, testdirs, test_as_current_user=True,
                                   other_user=None):
        self.assert_dirs_are(testdirs, test_as_current_user, other_user, True)

    def assert_dirs_are_inaccessible(self, testdirs, test_as_current_user=True,
                                     other_user=None):
        self.assert_dirs_are(testdirs, test_as_current_user, other_user, False)

    #######################################
    #
    # Test non-default, non-mask ACLs
    #
    ######################################

    def test_acl_for_file_owner(self):
        self.setup()
        self.mount_a.run_as_root('chown %s:%s %s' % (self.currentuser, self.currentgroup, self.testdir1))
        self.mount_a.run_shell('chmod u-rwx %s' % (self.testdir1))

        # preliminary test
        self.assert_dirs_are_inaccessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-m', self.fo_acl, self.testdir1])
        self.assert_acls_are_present(self.fo_acl, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

    def test_acl_for_file_group(self):
        self.setup()
        self.mount_a.run_as_root('chown %s:%s %s' % (self.currentuser, self.currentgroup, self.testdir1))
        self.mount_a.run_shell('chmod g-rwx %s' % (self.testdir1))

        # preliminary test
        self.assert_dirs_are_inaccessible(self.testdir1,
                                          test_as_current_user=False,
                                          other_user=self.currentuser2)

        self.mount_a.run_as_root(['setfacl', '-m', self.fg_acl, self.testdir1])
        self.assert_acls_are_present(self.fg_acl, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1,
                                        test_as_current_user=False,
                                        other_user=self.currentuser2)

    def test_acl_for_other(self):
        self.setup()
        self.mount_a.run_as_root('chmod o-rwx %s' % (self.testdir1))

        # preliminary test
        self.assert_dirs_are_inaccessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-m', self.o_acl, self.testdir1])
        self.assert_acls_are_present(self.o_acl, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

    def test_acl_for_current_user(self):
        self.setup()

        # preliminary test
        self.assert_dirs_are_inaccessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-m', self.u_acl, self.testdir1])
        self.assert_acls_are_present(self.u_acl, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-x', self.rm_u_acl, self.testdir1])
        self.assert_acls_are_absent(self.u_acl, self.testdir1)
        self.assert_dirs_are_inaccessible(self.testdir1)

    def test_acl_for_current_group(self):
        self.setup()

        # preliminary test
        self.assert_dirs_are_inaccessible(self.testdir1)

        self.mount_a.run_as_root('setfacl -m %s %s' % (self.g_acl, self.testdir1))
        self.assert_acls_are_present(self.g_acl, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-x', self.rm_g_acl, self.testdir1])
        self.assert_acls_are_absent(self.g_acl, self.testdir1)
        self.assert_dirs_are_inaccessible(self.testdir1)

    # XXX: Always fails. not rewritten since ages!
    def _test_acl_for_other_user(self):
        self.setup()
        acl = 'user:dummy:rwx'
        removeacl = 'user:dummy:'

        # preliminary test
        proc = self.mount_a.test_create_file(dirname=self.testdir1)
        assert proc.returncode != 0
        assert proc.stderr.getvalue().lower().find('permission denied') != -1

        self.mount_a.run_as_root(["setfacl", "-m", acl, self.testdir1])
        output = self.mount_a.run_shell(['getfacl', self.testdir1]).stdout.getvalue()
        assert output.find(acl) != -1
        proc = self.mount_a.testcmd_as_user('ls ' + self.testdir1, user=self.otheruser)
        assert proc.returncode == 0

        self.mount_a.run_as_root(['setfacl', '-x', removeacl, self.testdir1])
        output = self.mount_a.run_shell(['getfacl', self.testdir1]).stdout.getvalue()
        assert output.find(acl) == -1
        proc = self.mount_a.test_create_file(dirname=self.testdir1)
        assert proc.returncode != 0
        assert proc.stderr.getvalue().lower().find('permission denied') != -1

    # XXX: Always fails. not rewritten since ages!
    def _test_acl_for_other_group(self):
        self.setup()
        acl = 'group:dummy:rwx'
        removeacl = 'group:dummy:'

        # preliminary test
        proc = self.mount_a.test_create_file(dirname=self.testdir1)
        assert proc.returncode != 0
        assert proc.stderr.getvalue().lower().find('permission denied') != -1

        self.mount_a.run_as_root(['setfacl', '-m', acl, self.testdir1])
        self.mount_a.run_as_root(['getfacl', self.testdir1])
        output = self.mount_a.run_as_root(['getfacl', self.testdir1]).stdout.getvalue()
        assert output.find(acl) != -1
        proc = self.mount_a.test_create_file(dirname=self.testdir1)
        assert proc.returncode == 0

        self.mount_a.run_as_root(['setfacl', '-x', removeacl, self.testdir1])
        output = self.mount_a.run_as_root(['getfacl', self.testdir1]).stdout.getvalue()
        assert output.find(acl) == -1
        proc = self.mount_a.test_create_file(dirname=self.testdir1)
        assert proc.returncode == 0
        assert proc.stderr.getvalue().lower().find('permission denied') != -1

    ##################################################
    #
    # Test non-default, mask ACLs
    #
    ##################################################

    def run_acl_mask_test(self, acl, rm_acl):
        acls = [acl, self.m_acl]
        rm_acls = [rm_acl, self.rm_m_acl]

        # preliminary test
        self.mount_a.run_as_root(['setfacl', '-m', acl, self.testdir1])
        if acl == self.fg_acl:
            self.assert_dirs_are_accessible(self.testdir1,
                                            test_as_current_user=False,
                                            other_user=self.currentuser2)
        else:
            self.assert_dirs_are_accessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-m', self.m_acl, self.testdir1])
        self.assert_acls_are_present(acls, self.testdir1)
        if acl in [self.fo_acl, self.o_acl]: # since ACL_USER_OBJ and
                                             # ACL_OTHER are not affected by
                                             # masks
            self.assert_dirs_are_accessible(self.testdir1)
        elif acl == self.fg_acl:
            self.assert_dirs_are_inaccessible(self.testdir1,
                                              test_as_current_user=False,
                                              other_user=self.currentuser2)
        else:
            self.assert_dirs_are_inaccessible(self.testdir1)

        if rm_acl is not None:      # i.e. if acl is self.du_acl, self.dg_acl
            self.mount_a.run_as_root(['setfacl', '-x', rm_acl, self.testdir1])
            self.mount_a.run_as_root(['setfacl', '-x', self.rm_m_acl, self.testdir1])
            self.assert_acls_are_absent(acls, self.testdir1)
            self.assert_dirs_are_inaccessible(self.testdir1)

    def test_acl_mask_for_file_owner(self):
        self.setup()
        self.mount_a.run_as_root('chown %s:%s %s' % (self.currentuser, self.currentgroup, self.testdir1))
        self.mount_a.run_shell('chmod u-rwx %s' % (self.testdir1))

        self.run_acl_mask_test(self.fo_acl, None)

    def test_acl_mask_for_file_group(self):
        self.setup()
        self.mount_a.run_as_root('chown %s:%s %s' % (self.currentuser, self.currentgroup, self.testdir1))
        self.mount_a.run_shell('chmod g-rwx %s' % (self.testdir1))

        self.run_acl_mask_test(self.fg_acl, None)

    def test_acl_mask_for_other(self):
        self.setup()
        self.mount_a.run_as_root('chmod o-rwx %s' % (self.testdir1))

        self.run_acl_mask_test(self.o_acl, None)

    def test_acl_mask_for_current_user(self):
        self.setup()
        self.run_acl_mask_test(self.u_acl, self.rm_u_acl)

    def test_acl_mask_for_current_group(self):
        self.setup()
        self.run_acl_mask_test(self.g_acl, self.rm_g_acl)

    ########################################
    #
    # Test nonmask, default ACLs
    #
    ######################################

    def run_default_acl_tests(self, acl):
        rm_acl = self.get_corresponding_rm_acl(acl)
        inherited_acl = acl[acl.find(':') + 1 : ]
        rm_inherited_acl = inherited_acl[ : inherited_acl.find(':', inherited_acl.find(':') + 1)] + ':'

        self.mount_a.run_as_root(['setfacl', '-m', acl, self.mount_a.mountpoint])
        if self.is_fo_acl(acl) or self.is_fg_acl(acl) or self.is_o_acl(acl):
            if self.is_o_acl(acl):
                self.mount_a.run_as_root('mkdir ' + self.testdir1)
            else:
                self.mount_a.run_shell('mkdir ' + self.testdir1)
            # test that default ACLs are inherited and that inherited ACLs are enforced.
            self.assert_acls_are_present(inherited_acl, self.testdir1)
            if self.is_fg_acl(acl):
                self.assert_dirs_are_inaccessible(self.testdir1,
                                                  test_as_current_user=False,
                                                  other_user=self.currentuser2)
            else:
                self.assert_dirs_are_inaccessible(self.testdir1)

            # test that user/group/other::rwx also works (to make sure that if
            # permission bits did not affect ACL testing).
            self.mount_a.run_as_root(['setfacl', '-m', acl.replace('---', 'rwx'), self.mount_a.mountpoint])
            testdir4 = os.path.join(self.mount_a.mountpoint, 'dir4')
            self.mount_a.run_shell('mkdir ' + testdir4)
            self.assert_acls_are_present(acl.replace('---', 'rwx'), testdir4)
            if self.is_fg_acl(acl):
                self.assert_dirs_are_accessible(testdir4,
                                                test_as_current_user=False,
                                                other_user=self.currentuser2)
            else:
                self.assert_dirs_are_accessible(testdir4)
        else:
            self.mount_a.run_as_root('setfacl -m default:other::--- ' + \
                                     self.mount_a.mountpoint)
            # XXX: change_perms is set to False because not only child directories
            # inherit ACL corresponding to default ACL but also they inherit the
            # ACL corresponding to its current permission modes which might make
            # the former ACL ineffective. See "OBJECT CREATION AND DEFAULT ACLs"
            # section under acls man page.
            self.create_test_dirs(change_perms=False)

            # test that default ACLs are inherited and that inherited ACLs are enforced.
            self.assert_acls_are_present(inherited_acl, self.testdir1)
            self.assert_dirs_are_accessible(self.testdir1)

            # test that removing inherited ACL disallows the access.
            self.mount_a.run_as_root(['setfacl', '-x', rm_inherited_acl, self.testdir1])
            self.assert_acls_are_absent(inherited_acl, self.testdir1)
            self.assert_dirs_are_inaccessible(self.testdir1)

            # test that default ACL can be removed.
            self.mount_a.run_as_root(['setfacl', '-x', rm_acl, self.testdir1])
            self.assert_acls_are_absent(acl, self.testdir1)
            self.assert_dirs_are_inaccessible(self.testdir1)

            # test that ACL inheritance can be stopped.
            testdir5 = os.path.join(self.mount_a.mountpoint, 'dir5')
            self.mount_a.run_as_root(['setfacl', '-x', rm_acl, self.mount_a.mountpoint])
            self.mount_a.run_as_root(['mkdir', testdir5])
            self.assert_acls_are_absent([acl, inherited_acl], testdir5)
            self.assert_dirs_are_inaccessible(testdir5)

    def test_default_acl_for_file_owner(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests('default:user::---')

    def test_default_acl_for_file_group(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests('default:group::---')

    def test_default_acl_for_other(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests('default:other::---')

    def test_default_acl_for_current_user(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests(self.du_acl)

    def test_default_acl_for_current_group(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests(self.dg_acl)

    # TODO: this section is untested as well as incomplete.
    ########################################
    #
    # Test default mask ACL
    #
    ######################################

    def run_default_mask_acl_tests(self, acl):
        rm_acl = self.get_corresponding_rm_acl(acl)
        inherited_acl = acl[acl.find(':') + 1 : ]
        rm_inherited_acl = inherited_acl[ : inherited_acl.find(':')] + '::'

        # test that mask ACL, being default, is inherited.
        self.mount_a.run_as_root(['setfacl', '-m', acl, self.mount_a.mountpoint])
        self.mount_a.run_as_root(['setfacl', '-m', self.dm_acl, self.mountpoint])
        # XXX: change_perms is set to False because not only child directories
        # inherit ACL corresponding to default ACL but also they inherit the
        # ACL corresponding to its current permission modes which might make
        # the former ACL ineffective. See "OBJECT CREATION AND DEFAULT ACLs"
        # section under acls man page.
        self.create_test_dirs(change_perms=False)
        self.assert_acls_are_present(inherited_acl, self.testdirs)
        self.assert_dirs_are_inaccessible(self.testdirs)

        # test that default mask ACLs are removed.
        self.mount_a.run_as_root(['setfacl', '-x', self.rm_dm_acl, self.testdir1])
        self.assert_acls_are_absent(self.rm_dm_acl, self.testdir1)
        self.assert_acls_are_present(inherited_acl, self.testdir1)
        self.assert_dirs_are_accessible(self.testdirs)

        # test that ACL inheritance for default mask ACLs has stopped.
        testdir3 = os.path.join(self.testdir1, 'dir3')
        self.mount_a.run_shell(['mkdir', testdir3])
        self.assert_acls_are_present(inherited_acl, self.testdir1)
        self.assert_dirs_are_inaccessbile(self.testdir3)

    def _test_default_mask_acl_for_file_owner(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests('default:user::---')

    def _test_default_mask_acl_for_file_group(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests('default:group::---')

    def _test_default_mask_acl_for_other(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests('default:other::---')

    def _test_default_mask_acl_for_current_user(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests(self.du_acl)

    def _test_default_mask_acl_for_current_group(self):
        self.setup(mkdir=False)

        self.run_default_acl_tests(self.dg_acl)

    ########################################
    #
    # Test ACL removal
    #
    ######################################

    def test_acl_remove_only_default(self):
        self.setup(change_grp_perms=False)
        for acl in self.all_acls:
            self.mount_a.run_as_root(['setfacl', '-m', acl, self.testdir1])

        # preliminary test
        self.assert_acls_are_present(self.default_acls, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

        self.mount_a.run_as_root(['setfacl', '-k', self.testdir1])
        self.assert_acls_are_absent(self.default_acls, self.testdir1)
        self.assert_acls_are_present(self.nondefault_acls, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

    def test_acl_remove_all_acls(self):
        self.setup(change_grp_perms=False)
        for acl in self.all_acls:
            self.mount_a.run_as_root(['setfacl', '-m', acl, self.testdir1])

        # preliminary test
        self.assert_acls_are_present(self.all_acls, self.testdir1)
        self.assert_dirs_are_accessible(self.testdir1)

        self.mount_a.run_as_root('setfacl -b ' + self.testdir1)
        self.assert_acls_are_absent(self.removable_acls, self.testdir1)

        # Since application of a mask renders the file group perms
        # ineffective (even if they allowed file group access), removal
        # of mask changes the perm bits to assure that the effective file
        # group perms stay the same.
        acls_present = self.nonremovable_acls[:]
        acls_present.remove('group::rwx')
        acls_present.append('group::---')
        self.assert_acls_are_present(acls_present, self.testdir1)

        # since other:rwx should be left
        self.assert_dirs_are_accessible(self.testdir1)
