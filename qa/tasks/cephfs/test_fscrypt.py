from io import StringIO
from os.path import basename
import random
import string
import time

from logging import getLogger

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.xfstests_dev import XFSTestsDev

log = getLogger(__name__)

class FSCryptTestCase(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    def setUp(self):
        super().setUp()

        self.protector = ''.join(random.choice(string.ascii_letters) for _ in range(8))
        self.key_file = "/tmp/key"
        self.path = "dir/"

        self.mount_a.run_shell_payload("sudo fscrypt --help")
        self.mount_a.run_shell_payload("sudo fscrypt setup --help")
        self.mount_a.run_shell_payload("sudo fscrypt setup --force --quiet")
        self.mount_a.run_shell_payload("sudo fscrypt status")
        self.mount_a.run_shell_payload(f"sudo fscrypt setup --quiet {self.mount_a.hostfs_mntpt}")
        self.mount_a.run_shell_payload("sudo fscrypt status")
        self.mount_a.run_shell_payload(f"sudo dd if=/dev/urandom of={self.key_file} bs=32 count=1")
        self.mount_a.run_shell_payload(f"mkdir -p {self.path}")
        self.mount_a.run_shell_payload(f"sudo fscrypt encrypt --quiet --source=raw_key --name={self.protector} --no-recovery --skip-unlock --key={self.key_file} {self.path}")
        self.mount_a.run_shell_payload(f"sudo fscrypt unlock --quiet --key=/tmp/key {self.path}")

    def tearDown(self):
        self.mount_a.run_shell_payload(f"sudo fscrypt purge --force --quiet {self.mount_a.hostfs_mntpt}")

        super().tearDown()

class TestFSCrypt(FSCryptTestCase):

    def test_fscrypt_basic_mount(self):
        """
        That fscrypt can be setup and ingest files.
        """

        self.mount_a.run_shell_payload(f"cp -av /usr/include {self.path}/")

class TestFSCryptRecovery(FSCryptTestCase):

    def test_fscrypt_journal_recovery(self):
        """
        That alternate_name can be recovered from the journal.
        """

        file = ''.join(random.choice(string.ascii_letters) for _ in range(255))

        self.mount_a.run_shell_payload(f"cd {self.path} && dd if=/dev/urandom of={file} bs=512 count=1 oflag=sync && sync . && stat {file}")

        def verify_alternate_name():
            J = self.fs.read_cache("/dir", depth=0)
            self.assertEqual(len(J), 1)
            inode = J[0]
            dirfrags = inode['dirfrags']
            self.assertEqual(len(dirfrags), 1)
            dirfrag = dirfrags[0]
            dentries = dirfrag['dentries']
            self.assertEqual(len(dentries), 1)
            # we don't know it's encrypted name, so we cannot verify that it's {file}
            dentry = dentries[0]
            name = basename(dentry['path'])
            # https://github.com/ceph/ceph-client/blob/fec50db7033ea478773b159e0e2efb135270e3b7/fs/ceph/crypto.h#L65-L90
            self.assertEqual(len(name), 240)
            alternate_name = dentry['alternate_name']
            self.assertGreater(len(alternate_name), 240)

        verify_alternate_name()

        self.fs.fail()

        self.fs.journal_tool(['event', 'recover_dentries', 'list'], 0)
        self.fs.journal_tool(['journal', 'reset', '--yes-i-really-really-mean-it'], 0)

        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        # load all inodes into cache (may be cleared by journal reset)
        self.mount_a.run_shell_payload(f"cd {self.path} && find")

        verify_alternate_name()

        self.mount_a.run_shell_payload(f"cd {self.path} && stat {file}")

class TestFSCryptRMW(FSCryptTestCase):
    CLIENTS_REQUIRED = 2
    def setUp(self):
        super().setUp()
        self.mount_b.run_shell_payload(f"sudo fscrypt unlock --quiet --key=/tmp/key {self.path}")

    def test_fscrypt_overwrite_block_boundary(self):
        """Test writing data with small, half write on previous block and trailing on new block"""

        file = f'{self.path}/file.log'

        size = 5529
        offset = 3379
        contents = 's' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)

        time.sleep(10)

        size = 4033
        offset = 4127
        contents = 't' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)

    def test_fscrypt_huge_hole(self):
        """Test writing data with huge hole, half write on previous block and trailing on new block"""

        file = f'{self.path}/file.log'

        size = 4096
        offset = 2147477504
        contents = 's' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)
        time.sleep(10)

        size = 8
        offset = 12
        contents = 't' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)

    def test_fscrypt_med_hole_write_boundary(self):
        """Test writing data past many holes on offset 0 of block"""

        file = f'{self.path}/file.log'

        #reproducing sys calls after ffsb bench has started
        size = 3192
        offset = 60653568
        contents = 's' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)

    def test_fscrypt_simple_rmw(self):
        """ Test simple rmw"""

        file = f'{self.path}/file.log'

        size = 32
        offset = 0
        contents = 's' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)

        size = 8
        offset = 8
        contents = 't' * size
        self.mount_a.write_file_ex(path=file, bs=1, data=contents, offset=offset)

        src_hash = self.mount_a.dir_checksum(path=file)
        dest_hash = self.mount_b.dir_checksum(path=file)

        if src_hash != dest_hash:
            raise ValueError

    def test_fscrypt_truncate_overwrite(self):
        """ Test copy smaller file -> larger file gets new file size"""

        file1 = f'{self.path}/file1.log'
        file2 = f'{self.path}/file2.log'
        expected_size = 1024

        self.mount_a.touch(file1)
        self.mount_a.touch(file2)

        self.mount_a.truncate(file1, 1048576)
        self.mount_a.truncate(file2, 1024)

        #simulate copy file2 -> file1
        self.mount_a.copy_file_range(file2, file1, 9223372035781033984)
        actual_size = self.mount_a.stat(file1)['st_size']

        if actual_size != expected_size:
            raise ValueError

    def test_fscrypt_truncate_path(self):
        """ Test overwrite/cp displays effective_size and not real size"""

        file = f'{self.path}/file.log'
        expected_size = 68686

        #fstest create test1 0644;
        self.mount_a.touch_os(file)

        #fstest truncate test1 68686;
        self.mount_a.truncate(file, expected_size)

        #fstest stat test1 size
        if self.mount_a.lstat(file)['st_size'] != expected_size:
            raise ValueError
        #stat above command returns 69632 instead of truncated value.

    def test_fscrypt_lchown_symlink(self):
        """ Test lchown to ensure target is set"""

        file1 = f'{self.path}/file1.log'

        self.mount_a.touch(file1)

        #fstest symlink file1 symlink1
        file2 = f'{self.path}/symlink'
        self.mount_a.symlink(file1, file2)

        #fstest lchown symlink1 135 579
        self.mount_a.lchown(file2, 1000, 1000)

        # ls -l
        #-rw-r--r--. 1 root root  0 Apr 22 18:11 file1
        #lrwxrwxrwx. 1  135  579 46 Apr 22 18:11 symlink1 -> ''$'\266\310''%'$'\005''W'$'\335''.'$'\355\211''kblD'$'\300''gq'$'\002\236\367''3'$'\255\201\001''Z6;'$'\221''&'$'\216\331\177''Q'
        ###if os.readlink(file2) != file1:
           ### raise Exception

    def test_fscrypt_900mhole_100mwrite(self):
        """ Test 900m hole 100m data write"""

        size = 100
        offset = 900

        files=[f'{self.path}/kfile.log', f'{self.path}/fuse_file.log']
        KERNEL_INDEX = 0
        FUSE_INDEX = 1

        self.mount_a.write_n_mb(files[KERNEL_INDEX], size, seek=offset)
        src_hash = self.mount_a.dir_checksum(path=files[KERNEL_INDEX])
        dest_hash = self.mount_b.dir_checksum(path=files[KERNEL_INDEX])

        if src_hash != dest_hash:
            raise ValueError

        self.mount_b.write_n_mb(files[FUSE_INDEX], size, seek=offset)
        src_hash = self.mount_b.dir_checksum(path=files[FUSE_INDEX])
        dest_hash = self.mount_a.dir_checksum(path=files[FUSE_INDEX])

        if src_hash != dest_hash:
            raise ValueError

    def test_fscrypt_1gwrite_400m600mwrite(self):
        """ Test 200M overwrite of 1G file"""

        file=f'{self.path}/file.log'

        self.mount_a.write_n_mb(file, 1000)
        self.mount_b.write_n_mb(file, 200, seek=400)
        client1_hash = self.mount_a.dir_checksum(path=file)
        client2_hash = self.mount_b.dir_checksum(path=file)

        if client1_hash != client2_hash:
            raise ValueError

    def test_fscrypt_truncate_ladder(self):
        """ Test truncate down from 1GB"""

        file = f'{self.path}/file.log'
        expected_sizes = [1024, 900, 500, 1]

        # define the truncate side and the read side
        tside = self.mount_a
        rside = self.mount_b

        tside.touch(file)

        for expected_size in expected_sizes:
            tside.truncate(file, expected_size)
            tside_size = tside.stat(file)['st_size']
            rside_size = rside.stat(file)['st_size']
            if tside_size != rside_size:
                raise ValueError

            #swap which client does the truncate
            tside, rside = rside, tside

    def strided_tests(self, fscrypt_block_size, write_size, num_writes, shared_file, fill):
        wside = self.mount_a
        rside = self.mount_b

        contents = fill * write_size * num_writes

        for i in range(num_writes):
            offset = i * write_size
            end_offset = offset + write_size
            strided_write = contents[offset:end_offset]
            s_size = len(strided_write)
            print(f"=============== {offset} to - {end_offset} size: {s_size} ==============")
            wside.write_file_ex(path=shared_file, data=strided_write, bs=1, offset=offset, sync=True)
            wside, rside = rside, wside

        shared_contents1 = wside.read_file(shared_file)
        shared_contents2 = rside.read_file(shared_file)

        if shared_contents1 != shared_contents2:
            raise ValueError

        if contents != shared_contents1:
            print(f"================= {contents} \n vs \n {shared_contents1}")
            raise ValueError

    def test_fscrypt_strided_small(self):
        """ Test strided i/o within a single fscrypt block"""

        fscrypt_block_size = 4096
        write_size = 256
        num_writes = 16
        shared_file = f'{self.path}/file.log'
        fill = 's'

        self.strided_tests(fscrypt_block_size, write_size, num_writes, shared_file, fill)

    def test_fscrypt_strided_regular_write(self):
        """ Test aligned strided i/o on fscrypt block"""

        fscrypt_block_size = 4096
        write_size = fscrypt_block_size
        num_writes = 16
        shared_file = f'{self.path}/file.log'
        fill = 's'

        self.strided_tests(fscrypt_block_size, write_size, num_writes, shared_file, fill)

    def test_unaligned_strided_write(self):
        """ Test unaligned strided i/o on fscrypt block"""

        fscrypt_block_size = 4096
        write_size = 4000
        num_writes = 16
        shared_file = f'{self.path}/file.log'
        fill = 's'

        self.strided_tests(fscrypt_block_size, write_size, num_writes, shared_file, fill)

class TestFSCryptXFS(XFSTestsDev):

    def setup_xfsprogs_devs(self):
        self.install_xfsprogs = True

    def test_fscrypt_encrypt(self):
        # XXX: check_status is set to False so that we can check for command's
        # failure on our own (since this command doesn't set right error code
        # and error message in some cases) and print custom log messages
        # accordingly.
        proc = self.mount_a.client_remote.run(args=['sudo', 'env', 'DIFF_LENGTH=0',
            './check', '-g', 'encrypt'], cwd=self.xfstests_repo_path, stdout=StringIO(),
            stderr=StringIO(), timeout=900, check_status=False, omit_sudo=False,
            label='running tests for encrypt from xfstests-dev')

        if proc.returncode != 0:
            log.info('Command failed.')
        log.info(f'Command return value: {proc.returncode}')
        stdout, stderr = proc.stdout.getvalue(), proc.stderr.getvalue()
        log.info(f'Command stdout -\n{stdout}')
        log.info(f'Command stderr -\n{stderr}')

        # Currently only the 395,396,397,421,429,435,440,580,593,595 and 598
        # of the 26 test cases will be actually ran, all the others will be
        # skipped for now because of not supporting features in kernel or kceph.
        self.assertEqual(proc.returncode, 0)
        self.assertIn('Passed all 26 tests', stdout)

    def test_fscrypt_dummy_encryption_with_quick_group(self):
        self.write_local_config('test_dummy_encryption')

        # XXX: check_status is set to False so that we can check for command's
        # failure on our own (since this command doesn't set right error code
        # and error message in some cases) and print custom log messages
        # accordingly. This will take a long time and set the timeout to 3 hours.
        proc = self.mount_a.client_remote.run(args=['sudo', 'env', 'DIFF_LENGTH=0',
            './check', '-g', 'quick', '-E', './ceph.exclude'], cwd=self.xfstests_repo_path,
            stdout=StringIO(), stderr=StringIO(), timeout=10800, check_status=False,
            omit_sudo=False, label='running tests for dummy_encryption from xfstests-dev')

        if proc.returncode != 0:
            log.info('Command failed.')
        log.info(f'Command return value: {proc.returncode}')
        stdout, stderr = proc.stdout.getvalue(), proc.stderr.getvalue()
        log.info(f'Command stdout -\n{stdout}')
        log.info(f'Command stderr -\n{stderr}')

        # Currently, many test cases will be skipped due to unsupported features,
        # but still will be marked as successful.
        self.assertEqual(proc.returncode, 0)
        self.assertIn('Passed all ', stdout)
