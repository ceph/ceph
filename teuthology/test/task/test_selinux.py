from mock import patch, Mock, DEFAULT

from teuthology.config import FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.task.selinux import SELinux


class TestSELinux(object):
    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.config = dict()

    @patch('teuthology.task.selinux.get_status')
    def test_host_exclusion(self, mock_get_status):
        mock_get_status.return_value = None
        with patch.multiple(
            Remote,
            os=DEFAULT,
            run=DEFAULT,
        ):
            self.ctx.cluster = Cluster()
            remote1 = Remote('remote1')
            remote1.os = Mock()
            remote1.os.package_type = 'rpm'
            self.ctx.cluster.add(remote1, ['role1'])
            remote2 = Remote('remote1')
            remote2.os = Mock()
            remote2.os.package_type = 'deb'
            self.ctx.cluster.add(remote2, ['role2'])
            task_config = dict()
            with SELinux(self.ctx, task_config) as task:
                remotes = task.cluster.remotes.keys()
                assert remotes == [remote1]

