from teuthology.dispatcher import supervisor
from unittest.mock import patch

class TestCheckReImageFailureMarkDown(object):
    def setup(self):
        self.the_function = supervisor.check_for_reimage_failures_and_mark_down

    def create_n_out_of_10_reimage_failed_jobs(self, n):
        ret_list = []
        for i in range(n):
            obj1 = {
              "failure_reason":"Error reimaging machines: Manually raised error"
              }
            ret_list.append(obj1)
        for j in range(10-n):
            obj2 = {"failure_reason":"Error something else: dummy"}
            ret_list.append(obj2)
        return ret_list

    @patch('teuthology.dispatcher.supervisor.shortname')
    @patch('teuthology.lock.ops.update_lock')
    @patch('teuthology.dispatcher.supervisor.requests')
    @patch('teuthology.dispatcher.supervisor.urljoin')
    @patch('teuthology.dispatcher.supervisor.teuth_config')
    def test_one_machine_ten_reimage_failed_jobs(
        self,
        m_t_config,
        m_urljoin,
        m_requests,
        mark_down,
        shortname
        ):
        targets = {'fakeos@rmachine061.front.sepia.ceph.com': 'ssh-ed25519'}
        m_requests.get.return_value.json.return_value = \
            self.create_n_out_of_10_reimage_failed_jobs(10)
        shortname.return_value = 'rmachine061'
        self.the_function(targets)
        assert mark_down.called

    @patch('teuthology.dispatcher.supervisor.shortname')
    @patch('teuthology.lock.ops.update_lock')
    @patch('teuthology.dispatcher.supervisor.requests')
    @patch('teuthology.dispatcher.supervisor.urljoin')
    @patch('teuthology.dispatcher.supervisor.teuth_config')
    def test_one_machine_seven_reimage_failed_jobs(
        self,
        m_t_config,
        m_urljoin,
        m_requests,
        mark_down,
        shortname,
        ):
        targets = {'fakeos@rmachine061.front.sepia.ceph.com': 'ssh-ed25519'}
        m_requests.get.return_value.json.return_value = \
            self.create_n_out_of_10_reimage_failed_jobs(7)
        shortname.return_value = 'rmachine061'
        self.the_function(targets)
        assert mark_down.called is False

    @patch('teuthology.dispatcher.supervisor.shortname')
    @patch('teuthology.lock.ops.update_lock')
    @patch('teuthology.dispatcher.supervisor.requests')
    @patch('teuthology.dispatcher.supervisor.urljoin')
    @patch('teuthology.dispatcher.supervisor.teuth_config')
    def test_two_machine_all_reimage_failed_jobs(
        self,
        m_t_config,
        m_urljoin,
        m_requests,
        mark_down,
        shortname,
        ):
        targets = {'fakeos@rmachine061.front.sepia.ceph.com': 'ssh-ed25519',
                   'fakeos@rmachine179.back.sepia.ceph.com': 'ssh-ed45333'}
        m_requests.get.return_value.json.side_effect = \
            [self.create_n_out_of_10_reimage_failed_jobs(10),
            self.create_n_out_of_10_reimage_failed_jobs(10)]
        shortname.return_value.side_effect = ['rmachine061', 'rmachine179']
        self.the_function(targets)
        assert mark_down.call_count == 2

    @patch('teuthology.dispatcher.supervisor.shortname')
    @patch('teuthology.lock.ops.update_lock')
    @patch('teuthology.dispatcher.supervisor.requests')
    @patch('teuthology.dispatcher.supervisor.urljoin')
    @patch('teuthology.dispatcher.supervisor.teuth_config')
    def test_two_machine_one_healthy_one_reimage_failure(
        self,
        m_t_config,
        m_urljoin,
        m_requests,
        mark_down,
        shortname,
        ):
        targets = {'fakeos@rmachine061.front.sepia.ceph.com': 'ssh-ed25519',
                   'fakeos@rmachine179.back.sepia.ceph.com': 'ssh-ed45333'}
        m_requests.get.return_value.json.side_effect = \
            [self.create_n_out_of_10_reimage_failed_jobs(0),
            self.create_n_out_of_10_reimage_failed_jobs(10)]
        shortname.return_value.side_effect = ['rmachine061', 'rmachine179']
        self.the_function(targets)
        assert mark_down.call_count == 1
        assert mark_down.call_args_list[0][0][0].startswith('rmachine179')

