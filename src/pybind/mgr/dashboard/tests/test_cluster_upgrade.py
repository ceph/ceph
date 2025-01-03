from ..controllers.cluster import ClusterUpgrade
from ..tests import ControllerTestCase, patch_orch
from ..tools import NotificationQueue, TaskManager


class ClusterUpgradeControllerTest(ControllerTestCase):
    URL_CLUSTER_UPGRADE = '/api/cluster/upgrade'

    @classmethod
    def setup_server(cls):
        NotificationQueue.start_queue()
        TaskManager.init()
        cls.setup_controllers([ClusterUpgrade])

    @classmethod
    def tearDownClass(cls):
        NotificationQueue.stop()

    def test_upgrade_list(self):
        result = ['17.1.0', '16.2.7', '16.2.6', '16.2.5', '16.1.4', '16.1.3']
        with patch_orch(True) as fake_client:
            fake_client.upgrades.list.return_value = result
            self._get('{}?image=quay.io/ceph/ceph:v16.1.0&tags=False&show_all_versions=False'
                      .format(self.URL_CLUSTER_UPGRADE))
            self.assertStatus(200)
            self.assertJsonBody(result)

    def test_start_upgrade(self):
        msg = "Initiating upgrade to 17.2.6"
        with patch_orch(True) as fake_client:
            fake_client.upgrades.start.return_value = msg
            payload = {
                'version': '17.2.6'
            }
            self._post('{}/start'.format(self.URL_CLUSTER_UPGRADE), payload)
            self.assertStatus(200)
            self.assertJsonBody(msg)

    def test_pause_upgrade(self):
        msg = "Paused upgrade to 17.2.6"
        with patch_orch(True) as fake_client:
            fake_client.upgrades.pause.return_value = msg
            self._put('{}/pause'.format(self.URL_CLUSTER_UPGRADE))
            self.assertStatus(200)
            self.assertJsonBody(msg)

    def test_resume_upgrade(self):
        msg = "Resumed upgrade to 17.2.6"
        with patch_orch(True) as fake_client:
            fake_client.upgrades.resume.return_value = msg
            self._put('{}/resume'.format(self.URL_CLUSTER_UPGRADE))
            self.assertStatus(200)
            self.assertJsonBody(msg)

    def test_stop_upgrade(self):
        msg = "Stopped upgrade to 17.2.6"
        with patch_orch(True) as fake_client:
            fake_client.upgrades.stop.return_value = msg
            self._put('{}/stop'.format(self.URL_CLUSTER_UPGRADE))
            self.assertStatus(200)
            self.assertJsonBody(msg)
