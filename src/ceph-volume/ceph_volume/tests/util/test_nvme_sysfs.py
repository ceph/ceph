from ceph_volume.util import nvme_sysfs


class TestBlockDeviceFactSysfsRelPath:
    def test_scsi_uses_standard_path_when_present(self, fake_filesystem):
        fake_filesystem.create_file('/sys/block/sda/device/vendor', contents='ATA')
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/sda', 'device/vendor'
        ) == 'device/vendor'

    def test_scsi_falls_back_when_no_vendor_file(self, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/sda/device')
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/sda', 'device/vendor'
        ) == 'device/vendor'

    def test_nvme_remapped_via_sysfs(self, fake_filesystem):
        fake_filesystem.create_file(
            '/sys/block/nvme0n1/device/nvme0/device/vendor',
            contents='Samsung',
        )
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/nvme0n1', 'device/vendor'
        ) == 'device/nvme0/device/vendor'

    def test_nvme_partition_remapped_via_sysfs(self, fake_filesystem):
        fake_filesystem.create_file(
            '/sys/block/nvme1n2p3/device/nvme1/device/model',
            contents='foo',
        )
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/nvme1n2p3', 'device/model'
        ) == 'device/nvme1/device/model'

    def test_nvme_nonstandard_controller_name(self, fake_filesystem):
        """Kernel may use names like nvme0c0 for the controller sysfs link."""
        fake_filesystem.create_file(
            '/sys/block/nvme0c0n1/device/nvme0c0/device/vendor',
            contents='vend',
        )
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/nvme0c0n1', 'device/vendor'
        ) == 'device/nvme0c0/device/vendor'

    def test_ambiguous_multiple_matches_returns_default_rel(self, fake_filesystem):
        fake_filesystem.create_file(
            '/sys/block/weird/device/nvme0/device/vendor', contents='a'
        )
        fake_filesystem.create_file(
            '/sys/block/weird/device/nvme1/device/vendor', contents='b'
        )
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/weird', 'device/vendor'
        ) == 'device/vendor'

    def test_non_controller_fact_unchanged(self):
        assert nvme_sysfs.block_device_fact_sysfs_rel_path(
            '/sys/block/nvme0n1', 'removable'
        ) == 'removable'


class TestIsWholeNvmeNamespaceName:
    def test_whole_namespace(self, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/nvme0n1/device/nvme0')
        assert nvme_sysfs.is_whole_nvme_namespace_name('nvme0n1') is True

    def test_partition_false(self, fake_filesystem):
        fake_filesystem.create_file('/sys/block/nvme0n1p1/partition', contents='1')
        fake_filesystem.create_dir('/sys/block/nvme0n1p1/device/nvme0')
        assert nvme_sysfs.is_whole_nvme_namespace_name('nvme0n1p1') is False

    def test_scsi_false(self, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/sda/device')
        assert nvme_sysfs.is_whole_nvme_namespace_name('sda') is False

    def test_no_nvme_controller_link_false(self, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/nvme0n1/device')
        assert nvme_sysfs.is_whole_nvme_namespace_name('nvme0n1') is False

    def test_missing_sysdir_false(self):
        assert nvme_sysfs.is_whole_nvme_namespace_name(
            'nvme0n1', _sys_block_path='/this/path/does/not/exist'
        ) is False
