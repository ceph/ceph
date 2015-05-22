import os
import yaml

from mock import patch, DEFAULT, Mock
from pytest import raises
from StringIO import StringIO

from teuthology.config import config, FakeNamespace
from teuthology.exceptions import CommandFailedError
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task import ansible
from teuthology.task.ansible import Ansible, CephLab

from . import TestTask


class TestAnsibleTask(TestTask):
    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['role1'])
        self.ctx.cluster.add(Remote('remote2'), ['role2'])
        self.ctx.config = dict()

    def test_setup(self):
        task_config = dict(
            playbook=[]
        )

        def fake_get_playbook(self):
            self.playbook_file = 'fake'

        with patch.multiple(
            ansible.Ansible,
            find_repo=DEFAULT,
            get_playbook=fake_get_playbook,
            get_inventory=DEFAULT,
            generate_hosts_file=DEFAULT,
            generate_playbook=Mock(side_effect=Exception),
        ):
            task = Ansible(self.ctx, task_config)
            task.setup()

    def test_setup_generate_playbook(self):
        task_config = dict(
            playbook=[]
        )
        with patch.multiple(
            ansible.Ansible,
            find_repo=DEFAULT,
            get_playbook=DEFAULT,
            get_inventory=DEFAULT,
            generate_hosts_file=DEFAULT,
            generate_playbook=DEFAULT,
        ):
            task = Ansible(self.ctx, task_config)
            task.setup()
            task.generate_playbook.assert_called_once_with()

    def test_find_repo_path(self):
        task_config = dict(
            repo='~/my/repo',
        )
        task = Ansible(self.ctx, task_config)
        task.find_repo()
        assert task.repo_path == os.path.expanduser(task_config['repo'])

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_path_remote(self, m_fetch_repo):
        task_config = dict(
            repo='git://fake_host/repo.git',
        )
        m_fetch_repo.return_value = '/tmp/repo'
        task = Ansible(self.ctx, task_config)
        task.find_repo()
        assert task.repo_path == os.path.expanduser('/tmp/repo')

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_http(self, m_fetch_repo):
        task_config = dict(
            repo='http://example.com/my/repo',
        )
        task = Ansible(self.ctx, task_config)
        task.find_repo()
        m_fetch_repo.assert_called_once_with(task_config['repo'], 'master')

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_git(self, m_fetch_repo):
        task_config = dict(
            repo='git@example.com/my/repo',
        )
        task = Ansible(self.ctx, task_config)
        task.find_repo()
        m_fetch_repo.assert_called_once_with(task_config['repo'], 'master')

    def test_playbook_none(self):
        task_config = dict()
        task = Ansible(self.ctx, task_config)
        with raises(KeyError):
            task.get_playbook()

    def test_playbook_wrong_type(self):
        task_config = dict(
            playbook=dict(),
        )
        task = Ansible(self.ctx, task_config)
        with raises(TypeError):
            task.get_playbook()

    def test_playbook_list(self):
        playbook = [
            dict(
                roles=['role1'],
            ),
        ]
        task_config = dict(
            playbook=playbook,
        )
        task = Ansible(self.ctx, task_config)
        task.get_playbook()
        assert task.playbook == playbook

    @patch.object(ansible.requests, 'get')
    def test_playbook_http(self, m_get):
        m_get.return_value = Mock()
        m_get.return_value.text = 'fake playbook text'
        playbook = "http://example.com/my_playbook.yml"
        task_config = dict(
            playbook=playbook,
        )
        task = Ansible(self.ctx, task_config)
        task.get_playbook()
        m_get.assert_called_once_with(playbook)

    def test_playbook_file(self):
        fake_playbook = [dict(fake_playbook=True)]
        fake_playbook_obj = StringIO(yaml.safe_dump(fake_playbook))
        task_config = dict(
            playbook='~/fake/playbook',
        )
        task = Ansible(self.ctx, task_config)
        with patch('teuthology.task.ansible.file', create=True) as m_file:
            m_file.return_value = fake_playbook_obj
            task.get_playbook()
        assert task.playbook == fake_playbook

    def test_playbook_file_missing(self):
        task_config = dict(
            playbook='~/fake/playbook',
        )
        task = Ansible(self.ctx, task_config)
        with raises(IOError):
            task.get_playbook()

    def test_inventory_none(self):
        task_config = dict(
            playbook=[]
        )
        task = Ansible(self.ctx, task_config)
        with patch.object(ansible.os.path, 'exists') as m_exists:
            m_exists.return_value = False
            task.get_inventory()
        assert task.inventory is None

    def test_inventory_path(self):
        inventory = '/my/inventory'
        task_config = dict(
            playbook=[],
            inventory=inventory,
        )
        task = Ansible(self.ctx, task_config)
        task.get_inventory()
        assert task.inventory == inventory
        assert task.generated_inventory is False

    def test_inventory_etc(self):
        task_config = dict(
            playbook=[]
        )
        task = Ansible(self.ctx, task_config)
        with patch.object(ansible.os.path, 'exists') as m_exists:
            m_exists.return_value = True
            task.get_inventory()
        assert task.inventory == '/etc/ansible/hosts'
        assert task.generated_inventory is False

    def test_generate_hosts_file(self):
        task_config = dict(
            playbook=[]
        )
        task = Ansible(self.ctx, task_config)
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
        assert hosts_file_obj.readlines() == ['remote1\n', 'remote2\n']

    def test_generate_playbook(self):
        playbook = [
            dict(
                roles=['role1', 'role2'],
            ),
        ]
        task_config = dict(
            playbook=playbook
        )
        task = Ansible(self.ctx, task_config)
        playbook_file_path = '/my/playbook/file'
        playbook_file_obj = StringIO()
        playbook_file_obj.name = playbook_file_path
        with patch.object(ansible, 'NamedTemporaryFile') as m_NTF:
            m_NTF.return_value = playbook_file_obj
            task.get_playbook()
            task.generate_playbook()
            m_NTF.assert_called_once_with(prefix="teuth_ansible_playbook_",
                                          delete=False)
        assert task.generated_playbook is True
        assert task.playbook_file == playbook_file_obj
        playbook_file_obj.seek(0)
        playbook_result = yaml.safe_load(playbook_file_obj)
        for play in playbook:
            play['hosts'] = 'all'
        assert playbook_result == playbook

    def test_execute_playbook(self):
        playbook = '/my/playbook'
        task_config = dict(
            playbook=playbook
        )
        fake_playbook = [dict(fake_playbook=True)]
        fake_playbook_obj = StringIO(yaml.safe_dump(fake_playbook))
        fake_playbook_obj.name = playbook

        task = Ansible(self.ctx, task_config)
        with patch('teuthology.task.ansible.file', create=True) as m_file:
            m_file.return_value = fake_playbook_obj
            task.setup()
        args = task._build_args()
        logger = StringIO()
        with patch.object(ansible.pexpect, 'run') as m_run:
            m_run.return_value = ('', 0)
            task.execute_playbook(_logfile=logger)
            m_run.assert_called_once_with(
                ' '.join(args),
                logfile=logger,
                withexitstatus=True,
                timeout=None,
            )

    def test_execute_playbook_fail(self):
        task_config = dict(
            playbook=[],
        )
        task = Ansible(self.ctx, task_config)
        task.setup()
        with patch.object(ansible.pexpect, 'run') as m_run:
            m_run.return_value = ('', 1)
            with raises(CommandFailedError):
                task.execute_playbook()

    def test_build_args_no_tags(self):
        task_config = dict(
            playbook=[],
        )
        task = Ansible(self.ctx, task_config)
        task.setup()
        args = task._build_args()
        assert '--tags' not in args

    def test_build_args_tags(self):
        task_config = dict(
            playbook=[],
            tags="user,pubkeys"
        )
        task = Ansible(self.ctx, task_config)
        task.setup()
        args = task._build_args()
        assert args.count('--tags') == 1
        assert args[args.index('--tags') + 1] == 'user,pubkeys'

    def test_teardown_inventory(self):
        task_config = dict(
            playbook=[],
        )
        task = Ansible(self.ctx, task_config)
        task.generated_inventory = True
        task.inventory = 'fake'
        with patch.object(ansible.os, 'remove') as m_remove:
            task.teardown()
            assert m_remove.called_once_with('fake')

    def test_teardown_playbook(self):
        task_config = dict(
            playbook=[],
        )
        task = Ansible(self.ctx, task_config)
        task.generated_playbook = True
        task.playbook_file = Mock()
        task.playbook_file.name = 'fake'
        with patch.object(ansible.os, 'remove') as m_remove:
            task.teardown()
            assert m_remove.called_once_with('fake')


class TestCephLabTask(TestTask):
    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['role1'])
        self.ctx.cluster.add(Remote('remote2'), ['role2'])
        self.ctx.config = dict()

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_http(self, m_fetch_repo):
        repo = os.path.join(config.ceph_git_base_url,
                            'ceph-cm-ansible.git')
        task = CephLab(self.ctx, dict())
        task.find_repo()
        m_fetch_repo.assert_called_once_with(repo, 'master')

    def test_playbook_file(self):
        fake_playbook = [dict(fake_playbook=True)]
        fake_playbook_obj = StringIO(yaml.safe_dump(fake_playbook))
        playbook = 'cephlab.yml'
        fake_playbook_obj.name = playbook
        task = CephLab(self.ctx, dict())
        task.repo_path = '/tmp/fake/repo'
        with patch('teuthology.task.ansible.file', create=True) as m_file:
            m_file.return_value = fake_playbook_obj
            task.get_playbook()
        assert task.playbook_file.name == playbook
