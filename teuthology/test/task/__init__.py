from mock import patch
from pytest import raises

from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task import Task


class TestTask(object):
    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.config = dict()

    def test_overrides(self):
        self.ctx.config['overrides'] = dict(
            task=dict(
                key_1='overridden',
            ),
        )
        task_config = dict(
            key_1='default',
            key_2='default',
        )
        with Task(self.ctx, task_config) as task:
            assert task.config['key_1'] == 'overridden'
            assert task.config['key_2'] == 'default'

    def test_hosts_no_filter(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['role1'])
        self.ctx.cluster.add(Remote('remote2'), ['role2'])
        task_config = dict()
        with Task(self.ctx, task_config) as task:
            task_hosts = task.cluster.remotes.keys()
            assert len(task_hosts) == 2
            assert sorted(host.name for host in task_hosts) == ['remote1',
                                                                'remote2']

    def test_hosts_no_results(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['role1'])
        task_config = dict(
            hosts=['role2'],
        )
        with raises(RuntimeError):
            with Task(self.ctx, task_config):
                pass

    def test_hosts_one_role(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['role1'])
        self.ctx.cluster.add(Remote('remote2'), ['role2'])
        task_config = dict(
            hosts=['role1'],
        )
        with Task(self.ctx, task_config) as task:
            task_hosts = task.cluster.remotes.keys()
            assert len(task_hosts) == 1
            assert task_hosts[0].name == 'remote1'

    def test_hosts_two_roles(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1'), ['role1'])
        self.ctx.cluster.add(Remote('remote2'), ['role2'])
        self.ctx.cluster.add(Remote('remote3'), ['role3'])
        task_config = dict(
            hosts=['role1', 'role3'],
        )
        with Task(self.ctx, task_config) as task:
            task_hosts = task.cluster.remotes.keys()
            assert len(task_hosts) == 2
            hostnames = [host.name for host in task_hosts]
            assert sorted(hostnames) == ['remote1', 'remote3']

    def test_hosts_two_hostnames(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1.example.com'), ['role1'])
        self.ctx.cluster.add(Remote('remote2.example.com'), ['role2'])
        self.ctx.cluster.add(Remote('remote3.example.com'), ['role3'])
        task_config = dict(
            hosts=['remote1', 'remote2.example.com'],
        )
        with Task(self.ctx, task_config) as task:
            task_hosts = task.cluster.remotes.keys()
            assert len(task_hosts) == 2
            hostnames = [host.name for host in task_hosts]
            assert sorted(hostnames) == ['remote1.example.com',
                                         'remote2.example.com']

    def test_hosts_one_role_one_hostname(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('remote1.example.com'), ['role1'])
        self.ctx.cluster.add(Remote('remote2.example.com'), ['role2'])
        self.ctx.cluster.add(Remote('remote3.example.com'), ['role3'])
        task_config = dict(
            hosts=['role1', 'remote2.example.com'],
        )
        with Task(self.ctx, task_config) as task:
            task_hosts = task.cluster.remotes.keys()
            assert len(task_hosts) == 2
            hostnames = [host.name for host in task_hosts]
            assert sorted(hostnames) == ['remote1.example.com',
                                         'remote2.example.com']

    @patch.object(Task, 'setup')
    def test_setup_called(self, m_setup):
        task_config = dict()
        with Task(self.ctx, task_config):
            m_setup.assert_called_once_with()

    @patch.object(Task, 'begin')
    def test_begin_called(self, m_begin):
        task_config = dict()
        with Task(self.ctx, task_config):
            m_begin.assert_called_once_with()

    @patch.object(Task, 'end')
    def test_end_called(self, m_end):
        task_config = dict()
        with Task(self.ctx, task_config):
            pass
        m_end.assert_called_once_with()

    @patch.object(Task, 'teardown')
    def test_teardown_called(self, m_teardown):
        task_config = dict()
        with Task(self.ctx, task_config):
            pass
        m_teardown.assert_called_once_with()

    def test_skip_teardown(self):
        task_config = dict(
            skip_teardown=True,
        )

        def fake_teardown(self):
            assert False

        with patch.object(Task, 'teardown', fake_teardown):
            with Task(self.ctx, task_config):
                pass
