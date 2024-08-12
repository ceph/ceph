from ceph_volume.util import encryption
from mock.mock import patch, Mock
import base64
import pytest


class TestNoWorkqueue:
    def setup_method(self):
        encryption.conf.dmcrypt_no_workqueue = None

    @patch('ceph_volume.util.encryption.process.call',
           Mock(return_value=(['cryptsetup 2.7.2 flags: UDEV BLKID KEYRING' \
                               'FIPS KERNEL_CAPI PWQUALITY '], [''], 0)))
    def test_set_dmcrypt_no_workqueue_true(self):
        encryption.set_dmcrypt_no_workqueue()
        assert encryption.conf.dmcrypt_no_workqueue

    @patch('ceph_volume.util.encryption.process.call',
           Mock(return_value=(['cryptsetup 2.0.0'], [''], 0)))
    def test_set_dmcrypt_no_workqueue_false(self):
        encryption.set_dmcrypt_no_workqueue()
        assert encryption.conf.dmcrypt_no_workqueue is None

    @patch('ceph_volume.util.encryption.process.call',
           Mock(return_value=([''], ['fake error'], 1)))
    def test_set_dmcrypt_no_workqueue_cryptsetup_version_fails(self):
        with pytest.raises(RuntimeError):
            encryption.set_dmcrypt_no_workqueue()

    @patch('ceph_volume.util.encryption.process.call',
           Mock(return_value=(['unexpected output'], [''], 0)))
    def test_set_dmcrypt_no_workqueue_pattern_not_found(self):
        with pytest.raises(RuntimeError):
            encryption.set_dmcrypt_no_workqueue()

    @patch('ceph_volume.util.encryption.process.call',
           Mock(return_value=([], [''], 0)))
    def test_set_dmcrypt_no_workqueue_index_error(self):
        with pytest.raises(RuntimeError):
            encryption.set_dmcrypt_no_workqueue()


class TestGetKeySize(object):
    def test_get_size_from_conf_default(self, conf_ceph_stub):
        conf_ceph_stub('''
        [global]
        fsid=asdf
        ''')
        assert encryption.get_key_size_from_conf() == '512'

    def test_get_size_from_conf_custom(self, conf_ceph_stub):
        conf_ceph_stub('''
        [global]
        fsid=asdf
        [osd]
        osd_dmcrypt_key_size=256
        ''')
        assert encryption.get_key_size_from_conf() == '256'

    def test_get_size_from_conf_custom_invalid(self, conf_ceph_stub):
        conf_ceph_stub('''
        [global]
        fsid=asdf
        [osd]
        osd_dmcrypt_key_size=1024
        ''')
        assert encryption.get_key_size_from_conf() == '512'

class TestStatus(object):

    def test_skips_unuseful_lines(self, stub_call):
        out = ['some line here', '  device: /dev/sdc1']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {'device': '/dev/sdc1'}

    def test_removes_extra_quotes(self, stub_call):
        out = ['some line here', '  device: "/dev/sdc1"']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {'device': '/dev/sdc1'}

    def test_ignores_bogus_lines(self, stub_call):
        out = ['some line here', '  ']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {}


class TestDmcryptClose(object):

    def test_mapper_exists(self, fake_run, fake_filesystem):
        file_name = fake_filesystem.create_file('mapper-device')
        encryption.dmcrypt_close(file_name.path)
        arguments = fake_run.calls[0]['args'][0]
        assert arguments[0] == 'cryptsetup'
        assert arguments[1] == 'remove'
        assert arguments[2].startswith('/')

    def test_mapper_does_not_exist(self, fake_run):
        file_name = '/path/does/not/exist'
        encryption.dmcrypt_close(file_name)
        assert fake_run.calls == []


class TestDmcryptKey(object):

    def test_dmcrypt(self):
        result = encryption.create_dmcrypt_key()
        assert len(base64.b64decode(result)) == 128

class TestLuksFormat(object):
    @patch('ceph_volume.util.encryption.process.call')
    def test_luks_format_command_with_default_size(self, m_call, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=abcd')
        expected = [
            'cryptsetup',
            '--batch-mode',
            '--key-size',
            '512',
            '--key-file',
            '-',
            'luksFormat',
            '/dev/foo'
        ]
        encryption.luks_format('abcd', '/dev/foo')
        assert m_call.call_args[0][0] == expected

    @patch('ceph_volume.util.encryption.process.call')
    def test_luks_format_command_with_custom_size(self, m_call, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=abcd\n[osd]\nosd_dmcrypt_key_size=256')
        expected = [
            'cryptsetup',
            '--batch-mode',
            '--key-size',
            '256',
            '--key-file',
            '-',
            'luksFormat',
            '/dev/foo'
        ]
        encryption.luks_format('abcd', '/dev/foo')
        assert m_call.call_args[0][0] == expected


class TestLuksOpen(object):
    @patch('ceph_volume.util.encryption.bypass_workqueue', return_value=False)
    @patch('ceph_volume.util.encryption.process.call')
    def test_luks_open_command_with_default_size(self, m_call, m_bypass_workqueue, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=abcd')
        expected = [
            'cryptsetup',
            '--key-size',
            '512',
            '--key-file',
            '-',
            '--allow-discards',
            'luksOpen',
            '/dev/foo',
            '/dev/bar'
        ]
        encryption.luks_open('abcd', '/dev/foo', '/dev/bar')
        assert m_call.call_args[0][0] == expected

    @patch('ceph_volume.util.encryption.bypass_workqueue', return_value=False)
    @patch('ceph_volume.util.encryption.process.call')
    def test_luks_open_command_with_custom_size(self, m_call, m_bypass_workqueue, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=abcd\n[osd]\nosd_dmcrypt_key_size=256')
        expected = [
            'cryptsetup',
            '--key-size',
            '256',
            '--key-file',
            '-',
            '--allow-discards',
            'luksOpen',
            '/dev/foo',
            '/dev/bar'
        ]
        encryption.luks_open('abcd', '/dev/foo', '/dev/bar')
        assert m_call.call_args[0][0] == expected
