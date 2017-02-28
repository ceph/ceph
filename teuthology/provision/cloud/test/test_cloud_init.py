from teuthology.config import config
from teuthology.provision import cloud

dummy_config = dict(
    providers=dict(
        my_provider=dict(
            driver='dummy',
            driver_args=dict(
                creds=0,
            ),
            conf_1='1',
            conf_2='2',
        )
    )
)


class DummyProvider(cloud.base.Provider):
        # For libcloud's dummy driver
        _driver_posargs = ['creds']

dummy_drivers = dict(
    provider=DummyProvider,
    provisioner=cloud.base.Provisioner,
)


class TestInit(object):
    def setup(self):
        config.load()
        config.libcloud = dummy_config
        cloud.supported_drivers['dummy'] = dummy_drivers

    def teardown(self):
        del cloud.supported_drivers['dummy']

    def test_get_types(self):
        assert cloud.get_types() == ['my_provider']

    def test_get_provider_conf(self):
        expected = dummy_config['providers']['my_provider']
        assert cloud.get_provider_conf('my_provider') == expected

    def test_get_provider(self):
        obj = cloud.get_provider('my_provider')
        assert obj.name == 'my_provider'
        assert obj.driver_name == 'dummy'

    def test_get_provisioner(self):
        obj = cloud.get_provisioner(
            'my_provider',
            'node_name',
            'ubuntu',
            '16.04',
            dict(foo='bar'),
        )
        assert obj.provider.name == 'my_provider'
        assert obj.name == 'node_name'
        assert obj.os_type == 'ubuntu'
        assert obj.os_version == '16.04'
