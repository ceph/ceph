from mock import patch, DEFAULT
from pytest import raises

from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task import Task


class TestTask(object):
    klass = Task
    task_name = 'task'

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.config = dict()
        self.task_config = dict()

    def test_overrides(self):
        self.ctx.config['overrides'] = dict()
        self.ctx.config['overrides'][self.task_name] = dict(
            key_1='overridden',
        )
        self.task_config.update(dict(
            key_1='default',
            key_2='default',
        ))
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                assert task.config['key_1'] == 'overridden'
                assert task.config['key_2'] == 'default'

    def test_hosts_no_filter(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task_hosts = list(task.cluster.remotes)
                assert len(task_hosts) == 2
                assert sorted(host.shortname for host in task_hosts) == \
                    ['remote1', 'remote2']

    def test_hosts_no_results(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.task_config.update(dict(
            hosts=['role2'],
        ))
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with raises(RuntimeError):
                with self.klass(self.ctx, self.task_config):
                    pass

    def test_hosts_one_role(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        self.task_config.update(dict(
            hosts=['role1'],
        ))
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task_hosts = list(task.cluster.remotes)
                assert len(task_hosts) == 1
                assert task_hosts[0].shortname == 'remote1'

    def test_hosts_two_roles(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        self.ctx.cluster.add(Remote('user@remote3'), ['role3'])
        self.task_config.update(dict(
            hosts=['role1', 'role3'],
        ))
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task_hosts = list(task.cluster.remotes)
                assert len(task_hosts) == 2
                hostnames = [host.shortname for host in task_hosts]
                assert sorted(hostnames) == ['remote1', 'remote3']

    def test_hosts_two_hostnames(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1.example.com'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2.example.com'), ['role2'])
        self.ctx.cluster.add(Remote('user@remote3.example.com'), ['role3'])
        self.task_config.update(dict(
            hosts=['remote1', 'remote2.example.com'],
        ))
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task_hosts = list(task.cluster.remotes)
                assert len(task_hosts) == 2
                hostnames = [host.hostname for host in task_hosts]
                assert sorted(hostnames) == ['remote1.example.com',
                                             'remote2.example.com']

    def test_hosts_one_role_one_hostname(self):
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1.example.com'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2.example.com'), ['role2'])
        self.ctx.cluster.add(Remote('user@remote3.example.com'), ['role3'])
        self.task_config.update(dict(
            hosts=['role1', 'remote2.example.com'],
        ))
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task_hosts = list(task.cluster.remotes)
                assert len(task_hosts) == 2
                hostnames = [host.hostname for host in task_hosts]
                assert sorted(hostnames) == ['remote1.example.com',
                                             'remote2.example.com']

    def test_setup_called(self):
        with patch.multiple(
            self.klass,
            setup=DEFAULT,
            begin=DEFAULT,
            end=DEFAULT,
            teardown=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task.setup.assert_called_once_with()

    def test_begin_called(self):
        with patch.multiple(
            self.klass,
            setup=DEFAULT,
            begin=DEFAULT,
            end=DEFAULT,
            teardown=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task.begin.assert_called_once_with()

    def test_end_called(self):
        self.task_config.update(dict())
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                pass
            task.end.assert_called_once_with()

    def test_teardown_called(self):
        self.task_config.update(dict())
        with patch.multiple(
            self.klass,
            setup=DEFAULT,
            begin=DEFAULT,
            end=DEFAULT,
            teardown=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                pass
            task.teardown.assert_called_once_with()

    def test_skip_teardown(self):
        self.task_config.update(dict(
            skip_teardown=True,
        ))

        def fake_teardown(self):
            assert False

        with patch.multiple(
            self.klass,
            setup=DEFAULT,
            begin=DEFAULT,
            end=DEFAULT,
            teardown=fake_teardown,
        ):
            with self.klass(self.ctx, self.task_config):
                pass
