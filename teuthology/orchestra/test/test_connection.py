from mock import patch, Mock

from teuthology import config
from teuthology.orchestra import connection
from teuthology.orchestra.test.util import assert_raises


class TestConnection(object):
    def setup(self):
        self.start_patchers()

    def teardown(self):
        self.stop_patchers()

    def start_patchers(self):
        self.patcher_sleep = patch(
            'time.sleep',
        )
        self.patcher_sleep.start()
        self.m_ssh = Mock()
        self.patcher_ssh = patch(
            'teuthology.orchestra.connection.paramiko.SSHClient',
            self.m_ssh,
        )
        self.patcher_ssh.start()

    def stop_patchers(self):
        self.patcher_ssh.stop()
        self.patcher_sleep.stop()

    def clear_config(self):
        config.config.teuthology_yaml = ''
        config.config.load()

    def test_split_user_just_host(self):
        got = connection.split_user('somehost.example.com')
        assert got == (None, 'somehost.example.com')

    def test_split_user_both(self):
        got = connection.split_user('jdoe@somehost.example.com')
        assert got == ('jdoe', 'somehost.example.com')

    def test_split_user_empty_user(self):
        s = '@somehost.example.com'
        e = assert_raises(AssertionError, connection.split_user, s)
        assert str(e) == 'Bad input to split_user: {s!r}'.format(s=s)

    def test_connect(self):
        self.clear_config()
        config.config.verify_host_keys = True
        m_ssh_instance = self.m_ssh.return_value = Mock();
        m_transport = Mock()
        m_ssh_instance.get_transport.return_value = m_transport
        got = connection.connect(
            'jdoe@orchestra.test.newdream.net.invalid',
            _SSHClient=self.m_ssh,
        )
        self.m_ssh.assert_called_once()
        m_ssh_instance.set_missing_host_key_policy.assert_called_once()
        m_ssh_instance.load_system_host_keys.assert_called_once_with()
        m_ssh_instance.connect.assert_called_once_with(
            hostname='orchestra.test.newdream.net.invalid',
            username='jdoe',
            timeout=60,
        )
        m_transport.set_keepalive.assert_called_once_with(False)
        assert got is m_ssh_instance

    def test_connect_no_verify_host_keys(self):
        self.clear_config()
        config.config.verify_host_keys = False
        m_ssh_instance = self.m_ssh.return_value = Mock();
        m_transport = Mock()
        m_ssh_instance.get_transport.return_value = m_transport
        got = connection.connect(
            'jdoe@orchestra.test.newdream.net.invalid',
            _SSHClient=self.m_ssh,
        )
        self.m_ssh.assert_called_once()
        m_ssh_instance.set_missing_host_key_policy.assert_called_once()
        assert not m_ssh_instance.load_system_host_keys.called
        m_ssh_instance.connect.assert_called_once_with(
            hostname='orchestra.test.newdream.net.invalid',
            username='jdoe',
            timeout=60,
        )
        m_transport.set_keepalive.assert_called_once_with(False)
        assert got is m_ssh_instance

    def test_connect_override_hostkeys(self):
        self.clear_config()
        m_ssh_instance = self.m_ssh.return_value = Mock();
        m_transport = Mock()
        m_ssh_instance.get_transport.return_value = m_transport
        m_host_keys = Mock()
        m_ssh_instance.get_host_keys.return_value = m_host_keys
        m_create_key = Mock()
        m_create_key.return_value = "frobnitz"
        got = connection.connect(
            'jdoe@orchestra.test.newdream.net.invalid',
            host_key='ssh-rsa testkey',
            _SSHClient=self.m_ssh,
            _create_key=m_create_key,
            )
        self.m_ssh.assert_called_once()
        m_ssh_instance.get_host_keys.assert_called_once()
        m_host_keys.add.assert_called_once_with(
            hostname='orchestra.test.newdream.net.invalid',
            keytype='ssh-rsa',
            key='frobnitz',
        )
        m_create_key.assert_called_once_with('ssh-rsa', 'testkey')
        m_ssh_instance.connect.assert_called_once_with(
            hostname='orchestra.test.newdream.net.invalid',
            username='jdoe',
            timeout=60,
        )
        m_transport.set_keepalive.assert_called_once_with(False)
        assert got is m_ssh_instance
