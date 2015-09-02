from teuthology.config import config


class TestOpenStack(object):

    def setup(self):
        self.openstack_config = config['openstack']

    def test_config_clone(self):
        assert 'clone' in self.openstack_config

    def test_config_user_data(self):
        os_type = 'rhel'
        os_version = '7.0'
        template_path = self.openstack_config['user-data'].format(
            os_type=os_type,
            os_version=os_version)
        assert os_type in template_path
        assert os_version in template_path

    def test_config_ip(self):
        assert 'ip' in self.openstack_config

    def test_config_machine(self):
        assert 'machine' in self.openstack_config
        machine_config = self.openstack_config['machine']
        assert 'disk' in machine_config
        assert 'ram' in machine_config
        assert 'cpus' in machine_config

    def test_config_volumes(self):
        assert 'volumes' in self.openstack_config
        volumes_config = self.openstack_config['volumes']
        assert 'count' in volumes_config
        assert 'size' in volumes_config
