import six
import logging
from StringIO import StringIO
from tasks.cephfs.cephfs_test_case import CephFSTestCase

logger = logging.getLogger(__name__)


# TODO: add code to run non-ACL tests too.
# TODO: get tests running with SCRATCH_DEV and SCRATCH_DIR.
# TODO: make xfstests-dev tests running without running `make install`.
# TODO: make xfstests-dev compatible with ceph-fuse. xfstests-dev remounts
# CephFS before running tests using kernel, so ceph-fuse mounts are never
# actually testsed.
class XFSTestsDev(CephFSTestCase):

    def setUp(self):
        CephFSTestCase.setUp(self)
        self.prepare_xfstests_dev()

    def prepare_xfstests_dev(self):
        self.get_repo()
        self.get_test_and_scratch_dirs_ready()
        self.install_deps()
        self.create_reqd_users()
        self.write_local_config()

        # NOTE: On teuthology machines it's necessary to run "make" as
        # superuser since the repo is cloned somewhere in /tmp.
        self.mount_a.client_remote.run(args=['sudo', 'make'],
                                       cwd=self.repo_path, stdout=StringIO(),
                                       stderr=StringIO())
        self.mount_a.client_remote.run(args=['sudo', 'make', 'install'],
                                       cwd=self.repo_path, omit_sudo=False,
                                       stdout=StringIO(), stderr=StringIO())

    def get_repo(self):
        """
        Clone xfstests_dev repository. If already present, update it.
        """
        from teuthology.orchestra import run

        # TODO: make sure that repo is not cloned for every test. it should
        # happen only once.
        remoteurl = 'git://git.ceph.com/xfstests-dev.git'
        self.repo_path = self.mount_a.client_remote.mkdtemp(suffix=
                                                            'xfstests-dev')
        self.mount_a.run_shell(['git', 'archive', '--remote=' + remoteurl,
                                'HEAD', run.Raw('|'),
                                'tar', '-C', self.repo_path, '-x', '-f', '-'])

    def get_admin_key(self):
        import configparser
        from sys import version_info as sys_version_info

        cp = configparser.ConfigParser()
        if sys_version_info.major > 2:
            cp.read_string(self.fs.mon_manager.raw_cluster_cmd(
                'auth', 'get-or-create', 'client.admin'))
        # TODO: remove this part when we stop supporting Python 2
        elif sys_version_info.major <= 2:
            cp.read_string(six.text_type(self.fs.mon_manager.raw_cluster_cmd(
                'auth', 'get-or-create', 'client.admin')))

        return cp['client.admin']['key']

    def get_test_and_scratch_dirs_ready(self):
        """ "test" and "scratch" directories are directories inside Ceph FS.
            And, test and scratch mounts are path on the local FS where "test"
            and "scratch" directories would be mounted. Look at xfstests-dev
            local.config's template inside this file to get some context.
        """
        from os.path import join

        self.test_dirname = 'test'
        self.mount_a.run_shell(['mkdir', self.test_dirname])
        # read var name as "test dir's mount path"
        self.test_dirs_mount_path = self.mount_a.client_remote.mkdtemp(
            suffix=self.test_dirname)
        self.mount_a.run_shell(['sudo','ln','-s',join(self.mount_a.mountpoint,
                                                      self.test_dirname),
                                self.test_dirs_mount_path])

        self.scratch_dirname = 'scratch'
        self.mount_a.run_shell(['mkdir', self.scratch_dirname])
        # read var name as "scratch dir's mount path"
        self.scratch_dirs_mount_path = self.mount_a.client_remote.mkdtemp(
            suffix=self.scratch_dirname)
        self.mount_a.run_shell(['sudo','ln','-s',join(self.mount_a.mountpoint,
                                                      self.scratch_dirname),
                                self.scratch_dirs_mount_path])

    def install_deps(self):
        from teuthology.misc import get_system_type

        args = ['sudo', 'install', '-y']

        distro = get_system_type(self.mount_a.client_remote,
                                 distro=True).lower()
        if distro in ['redhatenterpriseserver', 'fedora', 'centos']:
            deps = """acl attr automake bc dbench dump e2fsprogs fio \
            gawk gcc indent libtool lvm2 make psmisc quota sed \
            xfsdump xfsprogs \
            libacl-devel libattr-devel libaio-devel libuuid-devel \
            xfsprogs-devel btrfs-progs-devel python sqlite""".split()
            deps_for_old_distros = "xfsprogs-qa-devel"

            args.insert(1, 'yum')
            args.extend(deps)
            args.append(deps_for_old_distros)
        elif distro == 'ubuntu':
            deps = """xfslibs-dev uuid-dev libtool-bin \
            e2fsprogs automake gcc libuuid1 quota attr libattr1-dev make \
            libacl1-dev libaio-dev xfsprogs libgdbm-dev gawk fio dbench \
            uuid-runtime python sqlite3""".split()

            args.insert(1, 'apt-get')
            args.extend(deps)
        else:
            raise RuntimeError('expected a yum based or a apt based system')

        self.mount_a.client_remote.run(args=args, omit_sudo=False)

    def create_reqd_users(self):
        self.mount_a.client_remote.run(args=['sudo', 'useradd', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'groupadd', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'useradd',
                                             '123456-fsgqa'], omit_sudo=False,
                                       check_status=False)

    def write_local_config(self):
        from os.path import join
        from textwrap import dedent
        from teuthology.misc import sudo_write_file

        mon_sock = self.fs.mon_manager.get_msgrv1_mon_socks()[0]
        self.test_dev = mon_sock + ':/' + self.test_dirname
        self.scratch_dev = mon_sock + ':/' + self.scratch_dirname

        xfstests_config_contents = dedent('''\
            export FSTYP=ceph
            export TEST_DEV={}
            export TEST_DIR={}
            #export SCRATCH_DEV={}
            #export SCRATCH_MNT={}
            export TEST_FS_MOUNT_OPTS="-o name=admin,secret={}"
            ''').format(self.test_dev, self.test_dirs_mount_path, self.scratch_dev,
                        self.scratch_dirs_mount_path, self.get_admin_key())

        sudo_write_file(self.mount_a.client_remote, join(
            self.repo_path, 'local.config'), xfstests_config_contents)

    def tearDown(self):
        self.mount_a.client_remote.run(args=['sudo', 'userdel', '--force',
                                             '--remove', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'userdel', '--force',
                                             '--remove', '123456-fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'groupdel', 'fsgqa'],
                                       omit_sudo=False, check_status=False)

        self.mount_a.client_remote.run(args=['sudo', 'rm', '-rf',
                                             self.repo_path],
                                       omit_sudo=False, check_status=False)
