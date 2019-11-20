from mock import patch, MagicMock
from pytest import skip
from teuthology.util.compat import PY3
if PY3:
    from io import StringIO as StringIO
else:
    from io import BytesIO as StringIO

from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task import ceph_ansible
from teuthology.task.ceph_ansible import CephAnsible

from teuthology.test.task import TestTask

SKIP_IRRELEVANT = "Not relevant to this subclass"


class TestCephAnsibleTask(TestTask):
    klass = CephAnsible
    task_name = 'ceph_ansible'

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['mon.0'])
        self.ctx.cluster.add(Remote('user@remote2'), ['mds.0'])
        self.ctx.cluster.add(Remote('user@remote3'), ['osd.0'])
        self.ctx.summary = dict()
        self.ctx.config = dict()
        self.task_config = dict()
        self.start_patchers()

    def start_patchers(self):
        m_fetch_repo = MagicMock()
        m_fetch_repo.return_value = 'PATH'

        def fake_get_scratch_devices(remote):
            return ['/dev/%s' % remote.shortname]

        self.patcher_get_scratch_devices = patch(
            'teuthology.task.ceph_ansible.get_scratch_devices',
            fake_get_scratch_devices,
        )
        self.patcher_get_scratch_devices.start()

        self.patcher_teardown = patch(
            'teuthology.task.ceph_ansible.CephAnsible.teardown',
        )
        self.patcher_teardown.start()

        def fake_set_iface_and_cidr(self):
            self._interface = 'eth0'
            self._cidr = '172.21.0.0/20'

        self.patcher_remote = patch.multiple(
            Remote,
            _set_iface_and_cidr=fake_set_iface_and_cidr,
        )
        self.patcher_remote.start()

    def stop_patchers(self):
        self.patcher_get_scratch_devices.stop()
        self.patcher_remote.stop()
        self.patcher_teardown.stop()

    def test_playbook_none(self):
        skip(SKIP_IRRELEVANT)

    def test_inventory_none(self):
        skip(SKIP_IRRELEVANT)

    def test_inventory_path(self):
        skip(SKIP_IRRELEVANT)

    def test_inventory_etc(self):
        skip(SKIP_IRRELEVANT)

    def test_generate_hosts_file(self):
        self.task_config.update(dict(
            playbook=[],
            vars=dict(
                osd_auto_discovery=True,
                monitor_interface='eth0',
                radosgw_interface='eth0',
                public_network='172.21.0.0/20',
            ),
        ))
        task = self.klass(self.ctx, self.task_config)
        hosts_file_path = '/my/hosts/file'
        hosts_file_obj = StringIO()
        hosts_file_obj.name = hosts_file_path
        with patch.object(ceph_ansible, 'NamedTemporaryFile') as m_NTF:
            m_NTF.return_value = hosts_file_obj
            task.generate_hosts_file()
            m_NTF.assert_called_once_with(prefix="teuth_ansible_hosts_",
                                          mode='w+',
                                          delete=False)
        assert task.generated_inventory is True
        assert task.inventory == hosts_file_path
        hosts_file_obj.seek(0)
        assert hosts_file_obj.read() == '\n'.join([
            '[mdss]',
            'remote2',
            '',
            '[mons]',
            'remote1',
            '',
            '[osds]',
            'remote3',
        ])

    def test_generate_hosts_file_with_devices(self):
        self.task_config.update(dict(
            playbook=[],
            vars=dict(
                monitor_interface='eth0',
                radosgw_interface='eth0',
                public_network='172.21.0.0/20',
            ),
        ))
        task = self.klass(self.ctx, self.task_config)
        hosts_file_path = '/my/hosts/file'
        hosts_file_obj = StringIO()
        hosts_file_obj.name = hosts_file_path
        with patch.object(ceph_ansible, 'NamedTemporaryFile') as m_NTF:
            m_NTF.return_value = hosts_file_obj
            task.generate_hosts_file()
            m_NTF.assert_called_once_with(prefix="teuth_ansible_hosts_",
                                          mode='w+',
                                          delete=False)
        assert task.generated_inventory is True
        assert task.inventory == hosts_file_path
        hosts_file_obj.seek(0)
        assert hosts_file_obj.read() == '\n'.join([
            '[mdss]',
            'remote2 devices=\'[]\'',
            '',
            '[mons]',
            'remote1 devices=\'[]\'',
            '',
            '[osds]',
            'remote3 devices=\'["/dev/remote3"]\'',
        ])

    def test_generate_hosts_file_with_network(self):
        self.task_config.update(dict(
            playbook=[],
            vars=dict(
                osd_auto_discovery=True,
            ),
        ))
        task = self.klass(self.ctx, self.task_config)
        hosts_file_path = '/my/hosts/file'
        hosts_file_obj = StringIO()
        hosts_file_obj.name = hosts_file_path
        with patch.object(ceph_ansible, 'NamedTemporaryFile') as m_NTF:
            m_NTF.return_value = hosts_file_obj
            task.generate_hosts_file()
            m_NTF.assert_called_once_with(prefix="teuth_ansible_hosts_",
                                          mode='w+',
                                          delete=False)
        assert task.generated_inventory is True
        assert task.inventory == hosts_file_path
        hosts_file_obj.seek(0)
        assert hosts_file_obj.read() == '\n'.join([
            '[mdss]',
            "remote2 monitor_interface='eth0' public_network='172.21.0.0/20' radosgw_interface='eth0'",
            '',
            '[mons]',
            "remote1 monitor_interface='eth0' public_network='172.21.0.0/20' radosgw_interface='eth0'",
            '',
            '[osds]',
            "remote3 monitor_interface='eth0' public_network='172.21.0.0/20' radosgw_interface='eth0'",
        ])
