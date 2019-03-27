import socket
import urlparse
import yaml

from copy import deepcopy
from libcloud.compute.providers import get_driver
from mock import patch, Mock, DEFAULT
from pytest import raises, mark

from teuthology.config import config
from teuthology.exceptions import MaxWhileTries
from teuthology.provision import cloud

test_config = dict(
    providers=dict(
        my_provider=dict(
            driver='openstack',
            driver_args=dict(
                username='user',
                password='password',
                ex_force_auth_url='http://127.0.0.1:9999/v2.0/tokens',
            ),
        )
    )
)


@patch('time.sleep')
def test_retry(m_sleep):
    orig_exceptions = cloud.openstack.RETRY_EXCEPTIONS
    new_exceptions = orig_exceptions + (RuntimeError, )

    class test_cls(object):
        def __init__(self, min_val):
            self.min_val = min_val
            self.cur_val = 0

        def func(self):
            self.cur_val += 1
            if self.cur_val < self.min_val:
                raise RuntimeError
            return self.cur_val

    with patch.object(
        cloud.openstack,
        'RETRY_EXCEPTIONS',
        new=new_exceptions,
    ):
        test_obj = test_cls(min_val=5)
        assert cloud.openstack.retry(test_obj.func) == 5
        test_obj = test_cls(min_val=1000)
        with raises(MaxWhileTries):
            cloud.openstack.retry(test_obj.func)


def get_fake_obj(mock_args=None, attributes=None):
    if mock_args is None:
        mock_args = dict()
    if attributes is None:
        attributes = dict()
    obj = Mock(**mock_args)
    for name, value in attributes.items():
        setattr(obj, name, value)
    return obj


class TestOpenStackBase(object):
    def setup(self):
        config.load()
        config.libcloud = deepcopy(test_config)
        self.start_patchers()

    def start_patchers(self):
        self.patchers = dict()
        self.patchers['m_list_images'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.list_images'
        )
        self.patchers['m_list_sizes'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.list_sizes'
        )
        self.patchers['m_ex_list_networks'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStack_1_1_NodeDriver.ex_list_networks'
        )
        self.patchers['m_ex_list_security_groups'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStack_1_1_NodeDriver.ex_list_security_groups'
        )
        self.patchers['m_get_user_ssh_pubkey'] = patch(
            'teuthology.provision.cloud.util.get_user_ssh_pubkey'
        )
        self.patchers['m_list_nodes'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.list_nodes'
        )
        self.patchers['m_create_node'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStack_1_1_NodeDriver.create_node'
        )
        self.patchers['m_wait_until_running'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.wait_until_running'
        )
        self.patchers['m_create_volume'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.create_volume'
        )
        self.patchers['m_attach_volume'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.attach_volume'
        )
        self.patchers['m_detach_volume'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.detach_volume'
        )
        self.patchers['m_list_volumes'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.list_volumes'
        )
        self.patchers['m_destroy_volume'] = patch(
            'libcloud.compute.drivers.openstack'
            '.OpenStackNodeDriver.destroy_volume'
        )
        self.patchers['m_get_service_catalog'] = patch(
            'libcloud.common.openstack'
            '.OpenStackBaseConnection.get_service_catalog'
        )
        self.patchers['m_auth_token'] = patch(
            'teuthology.provision.cloud.util.AuthToken'
        )
        self.patchers['m_get_endpoint'] = patch(
            'libcloud.common.openstack'
            '.OpenStackBaseConnection.get_endpoint',
        )
        self.patchers['m_sleep'] = patch(
            'time.sleep'
        )
        self.patchers['m_get'] = patch(
            'requests.get'
        )
        self.mocks = dict()
        for name, patcher in self.patchers.items():
            self.mocks[name] = patcher.start()
        self.mocks['m_get_endpoint'].return_value = 'endpoint'

    def teardown(self):
        for patcher in self.patchers.values():
            patcher.stop()


class TestOpenStackProvider(TestOpenStackBase):
    klass = cloud.openstack.OpenStackProvider

    def test_init(self):
        obj = cloud.get_provider('my_provider')
        assert obj.name == 'my_provider'
        assert obj.driver_name == 'openstack'
        assert obj.conf == test_config['providers']['my_provider']

    def test_driver(self):
        token = self.mocks['m_auth_token'].return_value
        self.mocks['m_auth_token'].return_value.__enter__.return_value = token
        token.value = None
        obj = cloud.get_provider('my_provider')
        assert isinstance(obj.driver, get_driver('openstack'))
        assert obj._auth_token.value is None

    def test_images(self):
        obj = cloud.get_provider('my_provider')
        self.mocks['m_list_images'].return_value = ['image0', 'image1']
        assert not hasattr(obj, '_images')
        assert obj.images == ['image0', 'image1']
        assert hasattr(obj, '_images')

    def test_sizes(self):
        obj = cloud.get_provider('my_provider')
        fake_sizes = [get_fake_obj(attributes=dict(name='size%s' % i)) for
                      i in range(2)]
        self.mocks['m_list_sizes'].return_value = fake_sizes
        assert not hasattr(obj, '_sizes')
        assert [s.name for s in obj.sizes] == ['size0', 'size1']
        assert hasattr(obj, '_sizes')

    def test_networks(self):
        obj = cloud.get_provider('my_provider')
        nets = [get_fake_obj(attributes=dict(name=i)) for i in ['net0', 'net1']]
        self.mocks['m_ex_list_networks'].return_value = nets
        assert not hasattr(obj, '_networks')
        assert [i.name for i in obj.networks] == [i.name for i in nets]
        assert hasattr(obj, '_networks')
        self.mocks['m_ex_list_networks'].side_effect = AttributeError
        obj = cloud.get_provider('my_provider')
        assert not hasattr(obj, '_networks')
        assert obj.networks == list()
        assert hasattr(obj, '_networks')

    def test_security_groups(self):
        obj = cloud.get_provider('my_provider')
        self.mocks['m_ex_list_security_groups'].return_value = ['sg0', 'sg1']
        assert not hasattr(obj, '_security_groups')
        assert obj.security_groups == ['sg0', 'sg1']
        assert hasattr(obj, '_security_groups')
        self.mocks['m_ex_list_security_groups'].side_effect = AttributeError
        obj = cloud.get_provider('my_provider')
        assert not hasattr(obj, '_security_groups')
        assert obj.security_groups == list()
        assert hasattr(obj, '_security_groups')


class TestOpenStackProvisioner(TestOpenStackBase):
    klass = cloud.openstack.OpenStackProvisioner

    def get_obj(
            self, name='node_name', os_type='ubuntu',
            os_version='16.04', conf=None):
        return cloud.get_provisioner(
            node_type='my_provider',
            name=name,
            os_type=os_type,
            os_version=os_version,
            conf=conf,
        )

    def test_init(self):
        with patch.object(
            self.klass,
            '_read_conf',
        ) as m_read_conf:
            self.get_obj()
            assert len(m_read_conf.call_args_list) == 1

    @mark.parametrize(
        'input_conf',
        [
            dict(machine=dict(
                disk=42,
                ram=9001,
                cpus=3,
            )),
            dict(volumes=dict(
                count=3,
                size=100,
            )),
            dict(),
            dict(
                machine=dict(
                    disk=1,
                    ram=2,
                    cpus=3,
                ),
                volumes=dict(
                    count=4,
                    size=5,
                )
            ),
            dict(
                machine=dict(
                    disk=100,
                ),
            ),
        ]
    )
    def test_read_conf(self, input_conf):
        obj = self.get_obj(conf=input_conf)
        for topic in ['machine', 'volumes']:
            combined = cloud.util.combine_dicts(
                [input_conf, config.openstack],
                lambda x, y: x > y,
            )
            assert obj.conf[topic] == combined[topic]

    @mark.parametrize(
        'input_conf, expected_machine, expected_vols',
        [
            [
                dict(openstack=[
                    dict(machine=dict(disk=64, ram=10000, cpus=3)),
                    dict(volumes=dict(count=1, size=1)),
                ]),
                dict(disk=64, ram=10000, cpus=3),
                dict(count=1, size=1),
            ],
            [
                dict(openstack=[
                    dict(machine=dict(cpus=3)),
                    dict(machine=dict(disk=1, ram=9000)),
                    dict(machine=dict(disk=50, ram=2, cpus=1)),
                    dict(machine=dict()),
                    dict(volumes=dict()),
                    dict(volumes=dict(count=0, size=0)),
                    dict(volumes=dict(count=1, size=0)),
                    dict(volumes=dict(size=1)),
                ]),
                dict(disk=50, ram=9000, cpus=3),
                dict(count=1, size=1),
            ],
            [
                dict(openstack=[
                    dict(volumes=dict(count=3, size=30)),
                    dict(volumes=dict(size=50)),
                ]),
                None,
                dict(count=3, size=50),
            ],
            [
                dict(openstack=[
                    dict(machine=dict(disk=100)),
                    dict(volumes=dict(count=3, size=30)),
                ]),
                dict(disk=100, ram=8000, cpus=1),
                dict(count=3, size=30),
            ],
        ]
    )
    def test_read_conf_legacy(
            self, input_conf, expected_machine, expected_vols):
        obj = self.get_obj(conf=input_conf)
        if expected_machine is not None:
            assert obj.conf['machine'] == expected_machine
        else:
            assert obj.conf['machine'] == config.openstack['machine']
        if expected_vols is not None:
            assert obj.conf['volumes'] == expected_vols

    @mark.parametrize(
        "os_type, os_version, should_find",
        [
            ('centos', '7', True),
            ('BeOS', '42', False),
        ]
    )
    def test_image(self, os_type, os_version, should_find):
        image_attrs = [
            dict(name='ubuntu-14.04'),
            dict(name='ubuntu-16.04'),
            dict(name='centos-7.0'),
        ]
        fake_images = list()
        for item in image_attrs:
            fake_images.append(
                get_fake_obj(attributes=item)
            )
        obj = self.get_obj(os_type=os_type, os_version=os_version)
        self.mocks['m_list_images'].return_value = fake_images
        if should_find:
            assert obj.os_version in obj.image.name
            assert obj.image in fake_images
        else:
            with raises(RuntimeError):
                obj.image

    @mark.parametrize(
        "input_attrs, func_or_exc",
        [
            (dict(ram=2**16),
             lambda s: s.ram == 2**16),
            (dict(disk=9999),
             lambda s: s.disk == 9999),
            (dict(cpus=99),
             lambda s: s.vcpus == 99),
            (dict(ram=2**16, disk=9999, cpus=99),
             IndexError),
        ]
    )
    def test_size(self, input_attrs, func_or_exc):
        size_attrs = [
            dict(ram=8000, disk=9999, vcpus=99, name='s0'),
            dict(ram=2**16, disk=20, vcpus=99, name='s1'),
            dict(ram=2**16, disk=9999, vcpus=1, name='s2'),
        ]
        fake_sizes = list()
        for item in size_attrs:
            fake_sizes.append(
                get_fake_obj(attributes=item)
            )
        base_spec = dict(machine=dict(
            ram=1,
            disk=1,
            cpus=1,
        ))
        spec = deepcopy(base_spec)
        spec['machine'].update(input_attrs)
        obj = self.get_obj(conf=spec)
        self.mocks['m_list_sizes'].return_value = fake_sizes
        if isinstance(func_or_exc, type):
            with raises(func_or_exc):
                obj.size
        else:
            assert obj.size in fake_sizes
            assert func_or_exc(obj.size) is True

    @mark.parametrize(
        "wanted_groups",
        [
            ['group1'],
            ['group0', 'group2'],
            [],
        ]
    )
    def test_security_groups(self, wanted_groups):
        group_names = ['group0', 'group1', 'group2']
        fake_groups = list()
        for name in group_names:
            fake_groups.append(
                get_fake_obj(attributes=dict(name=name))
            )
        self.mocks['m_ex_list_security_groups'].return_value = fake_groups
        obj = self.get_obj()
        assert obj.security_groups is None
        obj = self.get_obj()
        obj.provider.conf['security_groups'] = wanted_groups
        assert [g.name for g in obj.security_groups] == wanted_groups

    def test_security_groups_exc(self):
        fake_groups = [
            get_fake_obj(attributes=dict(name='sg')) for i in range(2)
        ]
        obj = self.get_obj()
        obj.provider.conf['security_groups'] = ['sg']
        with raises(RuntimeError):
            obj.security_groups
        self.mocks['m_ex_list_security_groups'].return_value = fake_groups
        obj = self.get_obj()
        obj.provider.conf['security_groups'] = ['sg']
        with raises(RuntimeError):
            obj.security_groups

    @mark.parametrize(
        "ssh_key",
        [
            'my_ssh_key',
            None,
        ]
    )
    def test_userdata(self, ssh_key):
        self.mocks['m_get_user_ssh_pubkey'].return_value = ssh_key
        obj = self.get_obj()
        userdata = yaml.safe_load(obj.userdata)
        assert userdata['user'] == obj.user
        assert userdata['hostname'] == obj.hostname
        if ssh_key:
            assert userdata['ssh_authorized_keys'] == [ssh_key]
        else:
            assert 'ssh_authorized_keys' not in userdata

    @mark.parametrize(
        'wanted_name, should_find, exception',
        [
            ('node0', True, None),
            ('node1', True, None),
            ('node2', False, RuntimeError),
            ('node3', False, None),
        ]
    )
    def test_node(self, wanted_name, should_find, exception):
        node_names = ['node0', 'node1', 'node2', 'node2']
        fake_nodes = list()
        for name in node_names:
            fake_nodes.append(
                get_fake_obj(attributes=dict(name=name))
            )
        self.mocks['m_list_nodes'].return_value = fake_nodes
        obj = self.get_obj(name=wanted_name)
        if should_find:
            assert obj.node.name == wanted_name
        elif exception:
            with raises(exception) as excinfo:
                obj.node
                assert excinfo.value.message
        else:
            assert obj.node is None

    @mark.parametrize(
        'networks, security_groups',
        [
            ([], []),
            (['net0'], []),
            ([], ['sg0']),
            (['net0'], ['sg0']),
        ]
    )
    def test_create(self, networks, security_groups):
        node_name = 'node0'
        fake_sizes = [
            get_fake_obj(
                attributes=dict(ram=2**16, disk=9999, vcpus=99, name='s0')),
        ]
        fake_security_groups = [
            get_fake_obj(attributes=dict(name=name))
            for name in security_groups
        ]
        self.mocks['m_ex_list_networks'].return_value = networks
        self.mocks['m_ex_list_security_groups'].return_value = \
            fake_security_groups
        self.mocks['m_list_sizes'].return_value = fake_sizes
        fake_images = [
            get_fake_obj(attributes=dict(name='ubuntu-16.04')),
        ]
        self.mocks['m_list_images'].return_value = fake_images
        self.mocks['m_get_user_ssh_pubkey'].return_value = 'ssh_key'
        fake_node = get_fake_obj(attributes=dict(name=node_name))
        fake_ips = ['555.123.4.0']
        self.mocks['m_create_node'].return_value = fake_node
        self.mocks['m_wait_until_running'].return_value = \
            [(fake_node, fake_ips)]
        obj = self.get_obj(name=node_name)
        obj._networks = networks
        obj.provider.conf['security_groups'] = security_groups
        p_wait_for_ready = patch(
            'teuthology.provision.cloud.openstack.OpenStackProvisioner'
            '._wait_for_ready'
        )
        with p_wait_for_ready:
            res = obj.create()
        assert res is obj.node
        # Test once again to ensure that if volume creation/attachment fails,
        # we destroy any remaining volumes and consider the node creation to
        # have failed as well.
        del obj._node
        with p_wait_for_ready:
            obj.conf['volumes']['count'] = 1
            obj.provider.driver.create_volume.side_effect = Exception
            with patch.object(obj, '_destroy_volumes'):
                assert obj.create() is False
                assert obj._destroy_volumes.called_once_with()

    def test_update_dns(self):
        config.nsupdate_url = 'nsupdate_url'
        obj = self.get_obj()
        obj.name = 'x'
        obj.ips = ['y']
        obj._update_dns()
        call_args = self.mocks['m_get'].call_args_list
        assert len(call_args) == 1
        url_base, query_string = call_args[0][0][0].split('?')
        assert url_base == 'nsupdate_url'
        parsed_query = urlparse.parse_qs(query_string)
        assert parsed_query == dict(name=['x'], ip=['y'])

    @mark.parametrize(
        'nodes',
        [[], [Mock()], [Mock(), Mock()]]
    )
    def test_destroy(self, nodes):
        with patch(
            'teuthology.provision.cloud.openstack.'
            'OpenStackProvisioner._find_nodes'
        ) as m_find_nodes:
            m_find_nodes.return_value = nodes
            obj = self.get_obj()
            result = obj.destroy()
            if not all(nodes):
                assert result is True
            else:
                for node in nodes:
                    assert node.destroy.called_once_with()

    _volume_matrix = (
        'count, size, should_succeed',
        [
            (1, 10, True),
            (0, 10, True),
            (10, 1, True),
            (1, 10, False),
            (10, 1, False),
        ]
    )

    @mark.parametrize(*_volume_matrix)
    def test_create_volumes(self, count, size, should_succeed):
        obj_conf = dict(volumes=dict(count=count, size=size))
        obj = self.get_obj(conf=obj_conf)
        node = get_fake_obj()
        if not should_succeed:
            obj.provider.driver.create_volume.side_effect = Exception
        obj._node = node
        result = obj._create_volumes()
        assert result is should_succeed
        if should_succeed:
            create_calls = obj.provider.driver.create_volume.call_args_list
            attach_calls = obj.provider.driver.attach_volume.call_args_list
            assert len(create_calls) == count
            assert len(attach_calls) == count
            for i in range(count):
                vol_size, vol_name = create_calls[i][0]
                assert vol_size == size
                assert vol_name == '%s_%s' % (obj.name, i)
                assert attach_calls[i][0][0] is obj._node
                assert attach_calls[i][1]['device'] is None

    @mark.parametrize(*_volume_matrix)
    def test_destroy_volumes(self, count, size, should_succeed):
        obj_conf = dict(volumes=dict(count=count, size=size))
        obj = self.get_obj(conf=obj_conf)
        fake_volumes = list()
        for i in range(count):
            vol_name = '%s_%s' % (obj.name, i)
            fake_volumes.append(
                get_fake_obj(attributes=dict(name=vol_name))
            )
        obj.provider.driver.list_volumes.return_value = fake_volumes
        obj._destroy_volumes()
        detach_calls = obj.provider.driver.detach_volume.call_args_list
        destroy_calls = obj.provider.driver.destroy_volume.call_args_list
        assert len(detach_calls) == count
        assert len(destroy_calls) == count
        assert len(obj.provider.driver.detach_volume.call_args_list) == count
        assert len(obj.provider.driver.destroy_volume.call_args_list) == count
        obj.provider.driver.detach_volume.reset_mock()
        obj.provider.driver.destroy_volume.reset_mock()
        obj.provider.driver.detach_volume.side_effect = Exception
        obj.provider.driver.destroy_volume.side_effect = Exception
        obj._destroy_volumes()
        assert len(obj.provider.driver.detach_volume.call_args_list) == count
        assert len(obj.provider.driver.destroy_volume.call_args_list) == count

    def test_destroy_volumes_exc(self):
        obj = self.get_obj()
        obj.provider.driver.detach_volume.side_effect = Exception

    def test_wait_for_ready(self):
        obj = self.get_obj()
        obj._node = get_fake_obj(attributes=dict(name='node_name'))
        with patch.multiple(
            'teuthology.orchestra.remote.Remote',
            connect=DEFAULT,
            run=DEFAULT,
        ) as mocks:
            obj._wait_for_ready()
            mocks['connect'].side_effect = socket.error
            with raises(MaxWhileTries):
                obj._wait_for_ready()
