from io import StringIO
from logging import getLogger
from os import getcwd as os_getcwd
from os.path import join
from textwrap import dedent


from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.kernel_mount import KernelMount


log = getLogger(__name__)


# TODO: add code to run non-ACL tests too.
# TODO: make xfstests-dev tests running without running `make install`.
class XFSTestsDev(CephFSTestCase):

    RESULTS_DIR = "results"

    def setUp(self):
        super(XFSTestsDev, self).setUp()
        self.setup_xfsprogs_devs()
        self.prepare_xfstests_devs()

    def setup_xfsprogs_devs(self):
        self.install_xfsprogs = False

    def prepare_xfstests_devs(self):
        # NOTE: To run a quick test with vstart_runner.py, enable next line
        # and disable calls to get_repo(), install_deps(), and
        # build_and_install() and also disable lines in tearDown() for repo
        # deletion.
        #self.xfstests_repo_path = '/path/to/xfstests-dev'

        self.get_repos()
        self.get_test_and_scratch_dirs_ready()
        self.install_deps()
        self.create_reqd_users()
        self.write_local_config()
        self.write_ceph_exclude()
        self.build_and_install()

    def tearDown(self):
        self.del_users_and_groups()
        self.del_repos()
        super(XFSTestsDev, self).tearDown()

    def del_users_and_groups(self):
        self.mount_a.client_remote.run(args=['sudo', 'userdel', '--force',
                                             '--remove', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'userdel', '--force',
                                             '--remove', '123456-fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'groupdel', 'fsgqa'],
                                       omit_sudo=False, check_status=False)

    def del_repos(self):
        self.save_results_dir()
        self.mount_a.client_remote.run(args=f'sudo rm -rf {self.xfstests_repo_path}',
                                       omit_sudo=False, check_status=False)

        if self.install_xfsprogs:
            self.mount_a.client_remote.run(args=f'sudo rm -rf {self.xfsprogs_repo_path}',
                                           omit_sudo=False, check_status=False)

    def save_results_dir(self):
        """
        When tests in xfstests-dev repo are executed, logs are created and
        saved, under a directory named "results" that lies at the repo root.
        In case a test from xfstests-dev repo fails, these logs will help find
        the cause of the failure.

        Since there's no option in teuthology to copy a directory lying at a
        custom location in order to save it from teuthology test runner's tear
        down, let's copy this directory to a standard location that teuthology
        copies away before erasing all data on the test machine. The standard
        location chosen in the case here is the Ceph log directory.

        In case of vstart_runner.py, this methods does nothing.
        """
        # No need to save results dir in case of vstart_runner.py.
        for x in ('LocalFuseMount', 'LocalKernelMount'):
            if x in self.mount_a.__class__.__name__:
                return

        src = join(self.xfstests_repo_path, self.RESULTS_DIR)

        if self.mount_a.run_shell(f'sudo stat {src}',
                check_status=False, omit_sudo=False).returncode != 0:
            log.info(f'xfstests-dev repo contains not directory named '
                     f'"{self.RESULTS_DIR}". repo location: {self.xfstests_repo_path}')
            return

        std_loc = '/var/log/ceph' # standard location
        dst = join(std_loc, 'xfstests-dev-results')
        self.mount_a.run_shell(f'sudo mkdir -p {dst}', omit_sudo=False)
        self.mount_a.run_shell(f'sudo cp -r {src} {dst}', omit_sudo=False)
        log.info(f'results dir from xfstests-dev has been saved; it was '
                 f'copied from {self.xfstests_repo_path} to {std_loc}.')

    def build_and_install(self):
        # NOTE: On teuthology machines it's necessary to run "make" as
        # superuser since the repo is cloned somewhere in /tmp.
        self.mount_a.client_remote.run(args=['sudo', 'make'],
                                       cwd=self.xfstests_repo_path, stdout=StringIO(),
                                       stderr=StringIO())
        self.mount_a.client_remote.run(args=['sudo', 'make', 'install'],
                                       cwd=self.xfstests_repo_path, omit_sudo=False,
                                       stdout=StringIO(), stderr=StringIO())

        if self.install_xfsprogs:
            self.mount_a.client_remote.run(args=['sudo', 'make'],
                                           cwd=self.xfsprogs_repo_path,
                                           stdout=StringIO(), stderr=StringIO())
            self.mount_a.client_remote.run(args=['sudo', 'make', 'install'],
                                           cwd=self.xfsprogs_repo_path, omit_sudo=False,
                                           stdout=StringIO(), stderr=StringIO())

    def get_repos(self):
        """
        Clone xfstests_dev and xfsprogs-dev repositories. If already present,
        update them. The xfsprogs-dev will be used to test the encrypt.
        """
        # TODO: make sure that repo is not cloned for every test. it should
        # happen only once.
        remoteurl = 'https://git.ceph.com/xfstests-dev.git'
        self.xfstests_repo_path = self.mount_a.client_remote.mkdtemp(suffix=
                                                            'xfstests-dev')
        self.mount_a.run_shell(['git', 'clone', remoteurl, '--depth', '1',
                                self.xfstests_repo_path])

        if self.install_xfsprogs:
            remoteurl = 'https://git.ceph.com/xfsprogs-dev.git'
            self.xfsprogs_repo_path = self.mount_a.client_remote.mkdtemp(suffix=
                                                                'xfsprogs-dev')
            self.mount_a.run_shell(['git', 'clone', remoteurl, '--depth', '1',
                                    self.xfsprogs_repo_path])

    def get_admin_key(self):
        import configparser

        cp = configparser.ConfigParser()
        cp.read_string(self.get_ceph_cmd_stdout('auth', 'get-or-create',
                                                'client.admin'))

        return cp['client.admin']['key']

    def get_test_and_scratch_dirs_ready(self):
        """ "test" and "scratch" directories are directories inside Ceph FS.
            And, test and scratch mounts are path on the local FS where "test"
            and "scratch" directories would be mounted. Look at xfstests-dev
            local.config's template inside this file to get some context.
        """
        self.test_dirname = 'test'
        self.mount_a.run_shell(['mkdir', self.test_dirname])
        # read var name as "test dir's mount path"
        self.test_dirs_mount_path = self.mount_a.client_remote.mkdtemp(
            suffix=self.test_dirname)

        self.scratch_dirname = 'scratch'
        self.mount_a.run_shell(['mkdir', self.scratch_dirname])
        # read var name as "scratch dir's mount path"
        self.scratch_dirs_mount_path = self.mount_a.client_remote.mkdtemp(
            suffix=self.scratch_dirname)

    def install_deps(self):
        from teuthology.misc import get_system_type

        distro, version = get_system_type(self.mount_a.client_remote,
                                          distro=True, version=True)
        distro = distro.lower()
        major_ver_num = int(version.split('.')[0]) # only keep major release
                                                   # number
        log.info(f'distro and version detected is "{distro}" and "{version}".')

        # we keep fedora here so that right deps are installed when this test
        # is run locally by a dev.
        if distro in ('redhatenterpriseserver', 'redhatenterprise', 'fedora',
                      'centos', 'centosstream', 'rhel'):
            deps = """acl attr automake bc dbench dump e2fsprogs fio \
            gawk gcc indent libtool lvm2 make psmisc quota sed \
            xfsdump xfsprogs \
            libacl-devel libattr-devel libaio-devel libuuid-devel \
            xfsprogs-devel btrfs-progs-devel python3 sqlite""".split()

            if self.install_xfsprogs:
                if distro == 'centosstream' and major_ver_num == 8:
                    deps += ['--enablerepo=powertools']
                deps += ['inih-devel', 'userspace-rcu-devel', 'libblkid-devel',
                         'gettext', 'libedit-devel', 'libattr-devel',
                         'device-mapper-devel', 'libicu-devel']

            deps_old_distros = ['xfsprogs-qa-devel']

            if distro != 'fedora' and major_ver_num > 7:
                    deps.remove('btrfs-progs-devel')

            args = ['sudo', 'yum', 'install', '-y'] + deps + deps_old_distros
        elif distro == 'ubuntu':
            deps = """xfslibs-dev uuid-dev libtool-bin \
            e2fsprogs automake gcc libuuid1 quota attr libattr1-dev make \
            libacl1-dev libaio-dev xfsprogs libgdbm-dev gawk fio dbench \
            uuid-runtime python sqlite3""".split()

            if self.install_xfsprogs:
                deps += ['libinih-dev', 'liburcu-dev', 'libblkid-dev',
                         'gettext', 'libedit-dev', 'libattr1-dev',
                         'libdevmapper-dev', 'libicu-dev', 'pkg-config']

            if major_ver_num >= 19:
                deps[deps.index('python')] ='python2'
            args = ['sudo', 'apt-get', 'install', '-y'] + deps
        else:
            raise RuntimeError('expected a yum based or a apt based system')

        self.mount_a.client_remote.run(args=args, omit_sudo=False)

    def create_reqd_users(self):
        self.mount_a.client_remote.run(args=['sudo', 'useradd', '-m', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'groupadd', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'useradd', 'fsgqa2'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'useradd',
                                             '123456-fsgqa'], omit_sudo=False,
                                       check_status=False)

    def write_local_config(self, options=None):
        if isinstance(self.mount_a, KernelMount):
            conf_contents = self._gen_conf_for_kernel_mnt(options)
        elif isinstance(self.mount_a, FuseMount):
            conf_contents = self._gen_conf_for_fuse_mnt(options)

        self.mount_a.client_remote.write_file(join(self.xfstests_repo_path,
                                                   'local.config'),
                                              conf_contents, sudo=True)
        log.info(f'local.config\'s contents -\n{conf_contents}')

    def _gen_conf_for_kernel_mnt(self, options=None):
        """
        Generate local.config for CephFS kernel client.
        """
        _options = '' if not options else ',' + options
        mon_sock = self.fs.mon_manager.get_msgrv1_mon_socks()[0]
        test_dev = mon_sock + ':/' + self.test_dirname
        scratch_dev = mon_sock + ':/' + self.scratch_dirname

        return dedent(f'''\
            export FSTYP=ceph
            export TEST_DEV={test_dev}
            export TEST_DIR={self.test_dirs_mount_path}
            export SCRATCH_DEV={scratch_dev}
            export SCRATCH_MNT={self.scratch_dirs_mount_path}
            export CEPHFS_MOUNT_OPTIONS="-o name=admin,secret={self.get_admin_key()}{_options}"
            ''')

    def _gen_conf_for_fuse_mnt(self, options=None):
        """
        Generate local.config for CephFS FUSE client.
        """
        mon_sock = self.fs.mon_manager.get_msgrv1_mon_socks()[0]
        test_dev = 'ceph-fuse'
        scratch_dev = ''
        # XXX: Please note that ceph_fuse_bin_path is not ideally required
        # because ceph-fuse binary ought to be present in one of the standard
        # locations during teuthology tests. But then testing with
        # vstart_runner.py will not work since ceph-fuse binary won't be
        # present in a standard locations during these sessions. Thus, this
        # workaround.
        ceph_fuse_bin_path = 'ceph-fuse' # bin expected to be in env
        if 'LocalFuseMount' in str(type(self.mount_a)): # for vstart_runner.py runs
            ceph_fuse_bin_path = join(os_getcwd(), 'bin', 'ceph-fuse')

        keyring_path = self.mount_a.client_remote.mktemp(
            data=self.fs.mon_manager.get_keyring('client.admin')+'\n')

        lastline = (f'export CEPHFS_MOUNT_OPTIONS="-m {mon_sock} -k '
                    f'{keyring_path} --client_mountpoint /{self.test_dirname}')
        lastline += f'-o {options}"' if options else '"'

        return dedent(f'''\
            export FSTYP=ceph-fuse
            export CEPH_FUSE_BIN_PATH={ceph_fuse_bin_path}
            export TEST_DEV={test_dev}  # without this tests won't get started
            export TEST_DIR={self.test_dirs_mount_path}
            export SCRATCH_DEV={scratch_dev}
            export SCRATCH_MNT={self.scratch_dirs_mount_path}
            {lastline}
            ''')

    def write_ceph_exclude(self):
        # These tests will fail or take too much time and will
        # make the test timedout, just skip them for now.
        xfstests_exclude_contents = dedent('''\
            {c}/001 {g}/003 {g}/020 {g}/075 {g}/317 {g}/538 {g}/531
            ''').format(g="generic", c="ceph")

        self.mount_a.client_remote.write_file(join(self.xfstests_repo_path, 'ceph.exclude'),
                                              xfstests_exclude_contents, sudo=True)
