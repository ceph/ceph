from mock import patch, Mock, DEFAULT

from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task.selinux import SELinux


class TestSELinux(object):
    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.config = dict()

    def test_host_exclusion(self):
        with patch.multiple(
            Remote,
            os=DEFAULT,
            run=DEFAULT,
        ):
            self.ctx.cluster = Cluster()
            remote1 = Remote('remote1')
            remote1.os = Mock()
            remote1.os.package_type = 'rpm'
            remote1._is_vm = False
            self.ctx.cluster.add(remote1, ['role1'])
            remote2 = Remote('remote1')
            remote2.os = Mock()
            remote2.os.package_type = 'deb'
            remote2._is_vm = False
            self.ctx.cluster.add(remote2, ['role2'])
            task_config = dict()
            with SELinux(self.ctx, task_config) as task:
                remotes = list(task.cluster.remotes)
                assert remotes == [remote1]

