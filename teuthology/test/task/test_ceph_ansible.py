from mock import patch, MagicMock
from pytest import skip
from StringIO import StringIO

from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task import ansible
from teuthology.task.ceph_ansible import CephAnsible

from .test_ansible import TestAnsibleTask

SKIP_IRRELEVANT = "Not relevant to this subclass"


class TestCephAnsibleTask(TestAnsibleTask):
    klass = CephAnsible
    task_name = 'ceph_ansible'

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['mon.0'])
        self.ctx.cluster.add(Remote('user@remote2'), ['mds.0'])
        self.ctx.cluster.add(Remote('user@remote3'), ['osd.0'])
        self.ctx.config = dict()
        self.task_config = dict()
        self.start_patchers()

    def start_patchers(self):
        super(TestCephAnsibleTask, self).start_patchers()
        m_fetch_repo = MagicMock()
        m_fetch_repo.return_value = 'PATH'
        self.patcher_fetch_repo = patch(
            'teuthology.task.ceph_ansible.ansible.fetch_repo',
            m_fetch_repo,
        )
        self.patcher_fetch_repo.start()

    def stop_patchers(self):
        super(TestCephAnsibleTask, self).stop_patchers()
        self.patcher_fetch_repo.stop()

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
            playbook=[]
        ))
        task = self.klass(self.ctx, self.task_config)
        hosts_file_path = '/my/hosts/file'
        hosts_file_obj = StringIO()
        hosts_file_obj.name = hosts_file_path
        with patch.object(ansible, 'NamedTemporaryFile') as m_NTF:
            m_NTF.return_value = hosts_file_obj
            task.generate_hosts_file()
            m_NTF.assert_called_once_with(prefix="teuth_ansible_hosts_",
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
