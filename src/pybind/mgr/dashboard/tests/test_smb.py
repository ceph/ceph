from unittest.mock import Mock

from dashboard.controllers.smb import SMBCluster, SMBShare

from .. import mgr
from ..tests import ControllerTestCase

class SMBClusterTest(ControllerTestCase):
    _endpoint = '/api/smb/cluster'

    _clusters = {
                "resources": [
                    {
                "resource_type": "ceph.smb.cluster",
                "cluster_id": "clusterADTest",
                "auth_mode": "active-directory",
                "intent": "present",
                "domain_settings": {
                    "realm": "DOMAIN1.SINK.TEST",
                    "join_sources": [
                        {
                            "source_type": "resource",
                            "ref": "join1-admin"
                        }]
                    },
                    "custom_dns": [
                        "192.168.76.204"
                    ],
                    "placement": {
                        "count": 1
                    }
                },
                {
                    "resource_type": "ceph.smb.cluster",
                    "cluster_id": "clusterUserTest",
                    "auth_mode": "user",
                    "intent": "present",
                    "user_group_settings": [
                        {
                        "source_type": "resource",
                        "ref": "ug1"
                        }
                    ]
                }
                ]
              }

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBCluster])

    def test_list(self):
        mgr.remote = Mock(return_value=self._clusters)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._clusters)

    def test_create_ad(self):
        mgr.remote = Mock(return_value=self._clusters['resources'][0])

        self._post(self._endpoint, {'cluster_id': 'clusterADTest', 'auth_mode': 'active-directory', 'domain_settings': {'realm': 'DOMAIN1.SINK.TEST', 'join_sources': ['join1-admin']}})
        self.assertStatus(201)
        self.assertInJsonBody(self._clusters['resources'][0])

    def test_create_user(self):
        mgr.remote = Mock(return_value=self._clusters['resources'][1])

        self._post(self._endpoint, {'cluster_id': 'clusterUserTest', 'auth_mode': 'user', 'user_group_settings': ['ug1']})
        self.assertStatus(201)
        self.assertInJsonBody(self._clusters['resources'][1])

class SMBShareTest(ControllerTestCase):
    _endpoint = '/api/smb/share'

    _shares = {
                "resources": [
                    {
                    "resource_type": "ceph.smb.share",
                    "cluster_id": "clusterUserTest",
                    "share_id": "share1",
                    "intent": "present",
                    "name": "share1name",
                    "readonly": "false",
                    "browseable": "true",
                    "cephfs": {
                        "volume": "fs1",
                        "path": "/",
                        "provider": "samba-vfs"
                    }
                    },
                    {
                    "resource_type": "ceph.smb.share",
                    "cluster_id": "clusterADTest",
                    "share_id": "share2",
                    "intent": "present",
                    "name": "share2name",
                    "readonly": "false",
                    "browseable": "true",
                    "cephfs": {
                        "volume": "fs2",
                        "path": "/",
                        "provider": "samba-vfs"
                    }
                    }
                ]
              }


    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBShare])

    def test_list_all(self):
        mgr.remote = Mock(return_value=self._shares)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._shares)

    def test_list_from_cluster(self):
        mgr.remote = Mock(return_value=self._shares['resources'][0])

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._shares['resources'][0])

    def test_delete(self):
        pass
        # TODO: set delete after share creation
        """
        _res = {
                "resource": {
                    "resource_type": "ceph.smb.share",
                    "cluster_id": "smbCluster1",
                    "share_id": "share1",
                    "intent": "removed"
                },
                "state": "removed",
                "success": "true"
            }
        mgr.remote = Mock(return_value=[_res])
        self._delete(self._endpoint)
        self.assertStatus(204)
        self.assertJsonBody(_res)
        """
