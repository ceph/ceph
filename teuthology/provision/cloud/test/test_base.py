from libcloud.compute.providers import get_driver
from mock import patch

from teuthology.config import config
from teuthology.provision import cloud

from test_cloud_init import dummy_config, dummy_drivers


class TestBase(object):
    def setup(self):
        config.load()
        config.libcloud = dummy_config
        cloud.supported_drivers['dummy'] = dummy_drivers

    def teardown(self):
        del cloud.supported_drivers['dummy']


class TestProvider(TestBase):
    def test_init(self):
        obj = cloud.get_provider('my_provider')
        assert obj.name == 'my_provider'
        assert obj.driver_name == 'dummy'
        assert obj.conf == dummy_config['providers']['my_provider']

    def test_driver(self):
        obj = cloud.get_provider('my_provider')
        assert isinstance(obj.driver, get_driver('dummy'))


class TestProvisioner(TestBase):
    klass = cloud.base.Provisioner

    def get_obj(
            self, name='node_name', os_type='ubuntu', os_version='ubuntu'):
        return cloud.get_provisioner(
            'my_provider',
            'node_name',
            'ubuntu',
            '16.04',
        )

    def test_init_provider_string(self):
        obj = self.klass('my_provider', 'ubuntu', '16.04')
        assert obj.provider.name == 'my_provider'

    def test_create(self):
        obj = self.get_obj()
        with patch.object(
            self.klass,
            '_create',
        ) as m_create:
            for val in [True, False]:
                m_create.return_value = val
                res = obj.create()
                assert res is val
                m_create.assert_called_once_with()
                m_create.reset_mock()
            m_create.side_effect = RuntimeError
            res = obj.create()
            assert res is False
        assert obj.create() is None

    def test_destroy(self):
        obj = self.get_obj()
        with patch.object(
            self.klass,
            '_destroy',
        ) as m_destroy:
            for val in [True, False]:
                m_destroy.return_value = val
                res = obj.destroy()
                assert res is val
                m_destroy.assert_called_once_with()
                m_destroy.reset_mock()
            m_destroy.side_effect = RuntimeError
            res = obj.destroy()
            assert res is False
        assert obj.destroy() is None

    def test_remote(self):
        obj = self.get_obj()
        assert obj.remote.shortname == 'node_name'

    def test_repr(self):
        obj = self.get_obj()
        assert repr(obj) == \
            "Provisioner(provider='my_provider', name='node_name', os_type='ubuntu', os_version='16.04')"  # noqa

