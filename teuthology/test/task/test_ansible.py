import json
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
    klass = Ansible
    task_name = 'ansible'

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        self.ctx.config = dict()
        self.task_config = dict(playbook=[])

    def test_setup(self):
        self.task_config.update(dict(
            playbook=[]
        ))

        def fake_get_playbook(self):
            self.playbook_file = 'fake'

        with patch.multiple(
            self.klass,
            find_repo=DEFAULT,
            get_playbook=fake_get_playbook,
            get_inventory=DEFAULT,
            generate_hosts_file=DEFAULT,
            generate_playbook=Mock(side_effect=Exception),
        ):
            task = self.klass(self.ctx, self.task_config)
            task.setup()

    def test_setup_generate_playbook(self):
        self.task_config.update(dict(
            playbook=[]
        ))
        with patch.multiple(
            self.klass,
            find_repo=DEFAULT,
            get_playbook=DEFAULT,
            get_inventory=DEFAULT,
            generate_hosts_file=DEFAULT,
            generate_playbook=DEFAULT,
        ):
            task = self.klass(self.ctx, self.task_config)
            task.setup()
            task.generate_playbook.assert_called_once_with()

    def test_find_repo_path(self):
        self.task_config.update(dict(
            repo='~/my/repo',
        ))
        task = self.klass(self.ctx, self.task_config)
        task.find_repo()
        assert task.repo_path == os.path.expanduser(self.task_config['repo'])

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_path_remote(self, m_fetch_repo):
        self.task_config.update(dict(
            repo='git://fake_host/repo.git',
        ))
        m_fetch_repo.return_value = '/tmp/repo'
        task = self.klass(self.ctx, self.task_config)
        task.find_repo()
        assert task.repo_path == os.path.expanduser('/tmp/repo')

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_http(self, m_fetch_repo):
        self.task_config.update(dict(
            repo='http://example.com/my/repo',
        ))
        task = self.klass(self.ctx, self.task_config)
        task.find_repo()
        m_fetch_repo.assert_called_once_with(self.task_config['repo'],
                                             'master')

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_git(self, m_fetch_repo):
        self.task_config.update(dict(
            repo='git@example.com/my/repo',
        ))
        task = self.klass(self.ctx, self.task_config)
        task.find_repo()
        m_fetch_repo.assert_called_once_with(self.task_config['repo'],
                                             'master')

    def test_playbook_none(self):
        del self.task_config['playbook']
        task = self.klass(self.ctx, self.task_config)
        with raises(KeyError):
            task.get_playbook()

    def test_playbook_wrong_type(self):
        self.task_config.update(dict(
            playbook=dict(),
        ))
        task = self.klass(self.ctx, self.task_config)
        with raises(TypeError):
            task.get_playbook()

    def test_playbook_list(self):
        playbook = [
            dict(
                roles=['role1'],
            ),
        ]
        self.task_config.update(dict(
            playbook=playbook,
        ))
        task = self.klass(self.ctx, self.task_config)
        task.get_playbook()
        assert task.playbook == playbook

    @patch.object(ansible.requests, 'get')
    def test_playbook_http(self, m_get):
        m_get.return_value = Mock()
        m_get.return_value.text = 'fake playbook text'
        playbook = "http://example.com/my_playbook.yml"
        self.task_config.update(dict(
            playbook=playbook,
        ))
        task = self.klass(self.ctx, self.task_config)
        task.get_playbook()
        m_get.assert_called_once_with(playbook)

    def test_playbook_file(self):
        fake_playbook = [dict(fake_playbook=True)]
        fake_playbook_obj = StringIO(yaml.safe_dump(fake_playbook))
        self.task_config.update(dict(
            playbook='~/fake/playbook',
        ))
        task = self.klass(self.ctx, self.task_config)
        with patch('teuthology.task.ansible.file', create=True) as m_file:
            m_file.return_value = fake_playbook_obj
            task.get_playbook()
        assert task.playbook == fake_playbook

    def test_playbook_file_missing(self):
        self.task_config.update(dict(
            playbook='~/fake/playbook',
        ))
        task = self.klass(self.ctx, self.task_config)
        with raises(IOError):
            task.get_playbook()

    def test_inventory_none(self):
        self.task_config.update(dict(
            playbook=[]
        ))
        task = self.klass(self.ctx, self.task_config)
        with patch.object(ansible.os.path, 'exists') as m_exists:
            m_exists.return_value = False
            task.get_inventory()
        assert task.inventory is None

    def test_inventory_path(self):
        inventory = '/my/inventory'
        self.task_config.update(dict(
            playbook=[],
            inventory=inventory,
        ))
        task = self.klass(self.ctx, self.task_config)
        task.get_inventory()
        assert task.inventory == inventory
        assert task.generated_inventory is False

    def test_inventory_etc(self):
        self.task_config.update(dict(
            playbook=[]
        ))
        task = self.klass(self.ctx, self.task_config)
        with patch.object(ansible.os.path, 'exists') as m_exists:
            m_exists.return_value = True
            task.get_inventory()
        assert task.inventory == '/etc/ansible/hosts'
        assert task.generated_inventory is False

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
        assert hosts_file_obj.readlines() == [
            'remote1\n',
            'remote2\n',
        ]

    def test_generate_playbook(self):
        playbook = [
            dict(
                roles=['role1', 'role2'],
            ),
        ]
        self.task_config.update(dict(
            playbook=playbook
        ))
        task = self.klass(self.ctx, self.task_config)
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
        self.task_config.update(dict(
            playbook=playbook
        ))
        fake_playbook = [dict(fake_playbook=True)]
        fake_playbook_obj = StringIO(yaml.safe_dump(fake_playbook))
        fake_playbook_obj.name = playbook

        task = self.klass(self.ctx, self.task_config)
        with patch('teuthology.task.ansible.file', create=True) as m_file:
            m_file.return_value = fake_playbook_obj
            task.setup()
        args = task._build_args()
        logger = StringIO()
        with patch.object(ansible.pexpect, 'run') as m_run:
            m_run.return_value = ('', 0)
            with patch.object(Remote, 'reconnect') as m_reconnect:
                m_reconnect.return_value = True
                task.execute_playbook(_logfile=logger)
            m_run.assert_called_once_with(
                ' '.join(args),
                logfile=logger,
                withexitstatus=True,
                timeout=None,
            )

    def test_execute_playbook_fail(self):
        self.task_config.update(dict(
            playbook=[],
        ))
        task = self.klass(self.ctx, self.task_config)
        task.setup()
        with patch.object(ansible.pexpect, 'run') as m_run:
            m_run.return_value = ('', 1)
            with raises(CommandFailedError):
                task.execute_playbook()

    def test_build_args_no_tags(self):
        self.task_config.update(dict(
            playbook=[],
        ))
        task = self.klass(self.ctx, self.task_config)
        task.setup()
        args = task._build_args()
        assert '--tags' not in args

    def test_build_args_tags(self):
        self.task_config.update(dict(
            playbook=[],
            tags="user,pubkeys"
        ))
        task = self.klass(self.ctx, self.task_config)
        task.setup()
        args = task._build_args()
        assert args.count('--tags') == 1
        assert args[args.index('--tags') + 1] == 'user,pubkeys'

    def test_build_args_no_vars(self):
        self.task_config.update(dict(
            playbook=[],
        ))
        task = self.klass(self.ctx, self.task_config)
        task.setup()
        args = task._build_args()
        assert args.count('--extra-vars') == 1
        vars_str = args[args.index('--extra-vars') + 1].strip("'")
        extra_vars = json.loads(vars_str)
        assert extra_vars.keys() == ['ansible_ssh_user']

    def test_build_args_vars(self):
        extra_vars = dict(
            string1='value1',
            list1=['item1'],
            dict1=dict(key='value'),
        )

        self.task_config.update(dict(
            playbook=[],
            vars=extra_vars,
        ))
        task = self.klass(self.ctx, self.task_config)
        task.setup()
        args = task._build_args()
        assert args.count('--extra-vars') == 1
        vars_str = args[args.index('--extra-vars') + 1].strip("'")
        got_extra_vars = json.loads(vars_str)
        assert 'ansible_ssh_user' in got_extra_vars
        assert got_extra_vars['string1'] == extra_vars['string1']
        assert got_extra_vars['list1'] == extra_vars['list1']
        assert got_extra_vars['dict1'] == extra_vars['dict1']

    def test_teardown_inventory(self):
        self.task_config.update(dict(
            playbook=[],
        ))
        task = self.klass(self.ctx, self.task_config)
        task.generated_inventory = True
        task.inventory = 'fake'
        with patch.object(ansible.os, 'remove') as m_remove:
            task.teardown()
            assert m_remove.called_once_with('fake')

    def test_teardown_playbook(self):
        self.task_config.update(dict(
            playbook=[],
        ))
        task = self.klass(self.ctx, self.task_config)
        task.generated_playbook = True
        task.playbook_file = Mock()
        task.playbook_file.name = 'fake'
        with patch.object(ansible.os, 'remove') as m_remove:
            task.teardown()
            assert m_remove.called_once_with('fake')

    def test_teardown_cleanup_with_vars(self):
        self.task_config.update(dict(
            playbook=[],
            cleanup=True,
            vars=dict(yum_repos="testing"),
        ))
        task = self.klass(self.ctx, self.task_config)
        task.inventory = "fake"
        task.generated_playbook = True
        task.playbook_file = Mock()
        task.playbook_file.name = 'fake'
        with patch.object(self.klass, 'execute_playbook') as m_execute:
            with patch.object(ansible.os, 'remove'):
                task.teardown()
            task._build_args()
            assert m_execute.called
            assert 'cleanup' in task.config['vars']
            assert 'yum_repos' in task.config['vars']

    def test_teardown_cleanup_with_no_vars(self):
        self.task_config.update(dict(
            playbook=[],
            cleanup=True,
        ))
        task = self.klass(self.ctx, self.task_config)
        task.inventory = "fake"
        task.generated_playbook = True
        task.playbook_file = Mock()
        task.playbook_file.name = 'fake'
        with patch.object(self.klass, 'execute_playbook') as m_execute:
            with patch.object(ansible.os, 'remove'):
                task.teardown()
            task._build_args()
            assert m_execute.called
            assert 'cleanup' in task.config['vars']


class TestCephLabTask(TestTask):
    klass = CephLab
    task_name = 'ansible.cephlab'

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        self.ctx.config = dict()
        self.task_config = dict()
        self.patcher_fetch_repo = patch('teuthology.task.ansible.fetch_repo')
        self.patcher_fetch_repo.return_value = 'PATH'
        self.patcher_fetch_repo.start()

        def fake_get_playbook(self):
            self.playbook_file = Mock()
            self.playbook_file.name = 'cephlab.yml'

        self.patcher_get_playbook = patch(
            'teuthology.task.ansible.CephLab.get_playbook',
            new=fake_get_playbook,
        )
        self.patcher_get_playbook.start()

    def teardown(self):
        self.patcher_fetch_repo.stop()
        self.patcher_get_playbook.stop()

    @patch('teuthology.task.ansible.fetch_repo')
    def test_find_repo_http(self, m_fetch_repo):
        repo = os.path.join(config.ceph_git_base_url,
                            'ceph-cm-ansible.git')
        task = self.klass(self.ctx, dict())
        task.find_repo()
        m_fetch_repo.assert_called_once_with(repo, 'master')

    def test_playbook_file(self):
        fake_playbook = [dict(fake_playbook=True)]
        fake_playbook_obj = StringIO(yaml.safe_dump(fake_playbook))
        playbook = 'cephlab.yml'
        fake_playbook_obj.name = playbook
        task = self.klass(self.ctx, dict())
        task.repo_path = '/tmp/fake/repo'
        with patch('teuthology.task.ansible.file', create=True) as m_file:
            m_file.return_value = fake_playbook_obj
            task.get_playbook()
        assert task.playbook_file.name == playbook


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
        assert hosts_file_obj.readlines() == [
            '[testnodes]\n',
            'remote1\n',
            'remote2\n',
        ]
