import json
from unittest.mock import Mock

from dashboard.controllers.smb import SMBCluster, SMBShare

from .. import mgr
from ..tests import ControllerTestCase


class SMBClusterTest(ControllerTestCase):
    _endpoint = '/api/smb/cluster'

    _clusters = {
        "resources": [{
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
        }]
    }

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBCluster])

    def test_list_one_cluster(self):
        mgr.remote = Mock(return_value=self._clusters['resources'][0])

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody([self._clusters['resources'][0]])

    def test_list_multiple_clusters(self):
        mgr.remote = Mock(return_value=self._clusters)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._clusters['resources'])

    def test_get_cluster(self):
        mgr.remote = Mock(return_value=self._clusters['resources'][0])
        cluster_id = self._clusters['resources'][0]['cluster_id']
        self._get(f'{self._endpoint}/{cluster_id}')
        self.assertStatus(200)
        self.assertJsonBody(self._clusters['resources'][0])
        mgr.remote.assert_called_once_with('smb', 'show', [f'ceph.smb.cluster.{cluster_id}'])

    def test_create_ad(self):
        mock_simplified = Mock()
        mock_simplified.to_simplified.return_value = json.dumps(self._clusters['resources'][0])
        mgr.remote = Mock(return_value=mock_simplified)

        _cluster_data = """
                        { "cluster_resource": {
                            "resource_type": "ceph.smb.cluster",
                            "cluster_id": "clusterADTest",
                            "auth_mode": "active-directory",
                            "domain_settings": {
                                "realm": "DOMAIN1.SINK.TEST",
                                "join_sources": [
                                    {
                                        "source_type": "resource",
                                        "ref": "join1-admin"
                                    }
                                ]
                            }
                        }
                        }
                        """

        self._post(self._endpoint, _cluster_data)
        self.assertStatus(201)
        self.assertInJsonBody(json.dumps(self._clusters['resources'][0]))

    def test_create_user(self):
        mock_simplified = Mock()
        mock_simplified.to_simplified.return_value = json.dumps(self._clusters['resources'][1])
        mgr.remote = Mock(return_value=mock_simplified)

        _cluster_data = """
                    { "cluster_resource": {
                        "resource_type": "ceph.smb.cluster",
                        "cluster_id": "clusterUser123Test",
                        "auth_mode": "user",
                        "user_group_settings": [
                            {
                                "source_type": "resource",
                                "ref": "ug1"
                            }
                        ]
                    }
                    }
                    """
        self._post(self._endpoint, _cluster_data)
        self.assertStatus(201)
        self.assertInJsonBody(json.dumps(self._clusters['resources'][1]))


class SMBShareTest(ControllerTestCase):
    _endpoint = '/api/smb/share'

    _shares = [{
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

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBShare])

    def test_list_all(self):
        mgr.remote = Mock(return_value=self._shares)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._shares)

    def test_list_from_cluster(self):
        mgr.remote = Mock(return_value=self._shares[0])

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._shares[0])

    def test_delete(self):
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
        _res_simplified = {
            "resource_type": "ceph.smb.share",
            "cluster_id": "smbCluster1",
            "share_id": "share1",
            "intent": "removed"
        }
        mgr.remote = Mock(return_value=Mock(return_value=_res))
        mgr.remote.return_value.one.return_value.to_simplified = Mock(return_value=_res_simplified)
        self._delete(f'{self._endpoint}/smbCluster1/share1')
        self.assertStatus(204)
        mgr.remote.assert_called_once_with('smb', 'apply_resources', json.dumps(_res_simplified))
