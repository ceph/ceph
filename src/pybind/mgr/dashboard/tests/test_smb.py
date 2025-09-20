import json
from unittest.mock import Mock

from dashboard.controllers.smb import SMBCluster, SMBJoinAuth, SMBShare, SMBUsersgroups

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

    def test_remove(self):
        _res = {
            "resource": {
                "resource_type": "ceph.smb.cluster",
                "cluster_id": "smbRemoveCluster",
                "intent": "removed"
            },
            "state": "removed",
            "success": "true"
        }
        _res_simplified = {
            "resource_type": "ceph.smb.cluster",
            "cluster_id": "smbRemoveCluster",
            "intent": "removed"
        }
        mgr.remote = Mock(return_value=Mock(return_value=_res))
        mgr.remote.return_value.one.return_value.to_simplified = Mock(return_value=_res_simplified)
        self._delete(f'{self._endpoint}/smbRemoveCluster')
        self.assertStatus(204)
        mgr.remote.assert_called_once_with('smb', 'apply_resources', json.dumps(_res_simplified))


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
                    "provider": "samba-vfs",
                },
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
                    "provider": "samba-vfs",
                },
            },
        ]
    }

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBShare])

    def test_list_all(self):
        mgr.remote = Mock(return_value=self._shares)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._shares['resources'])

    def test_list_one_share(self):
        mgr.remote = Mock(return_value=self._shares['resources'][0])

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody([self._shares['resources'][0]])

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


class SMBJoinAuthTest(ControllerTestCase):
    _endpoint = '/api/smb/joinauth'

    _join_auths = {
        "resources": [
            {
                "resource_type": "ceph.smb.join.auth",
                "auth_id": "join1-admin",
                "intent": "present",
                "auth": {
                    "username": "Administrator",
                    "password": "Passw0rd"
                }
            },
            {
                "resource_type": "ceph.smb.join.auth",
                "auth_id": "ja2",
                "intent": "present",
                "auth": {
                    "username": "user123",
                    "password": "foobar"
                }
            }
        ]
    }

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBJoinAuth])

    def test_list_one_join_auth(self):
        mgr.remote = Mock(return_value=self._join_auths['resources'][0])

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody([self._join_auths['resources'][0]])

    def test_list_multiple_clusters(self):
        mgr.remote = Mock(return_value=self._join_auths)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._join_auths['resources'])

    def test_create_join_auth(self):
        mock_simplified = Mock()
        mock_simplified.to_simplified.return_value = json.dumps(self._join_auths['resources'][0])
        mgr.remote = Mock(return_value=mock_simplified)

        _join_auth_data = {'join_auth': self._join_auths['resources'][0]}

        self._post(self._endpoint, _join_auth_data)
        self.assertStatus(201)
        self.assertInJsonBody(json.dumps(self._join_auths['resources'][0]))

    def test_delete(self):
        _res = {
            "resource_type": "ceph.smb.join.auth",
            "auth_id": "join1-admin",
            "intent": "removed",
            "auth": {
                "username": "Administrator",
                "password": "Passw0rd"
            }
        }
        _res_simplified = {
            "resource_type": "ceph.smb.join.auth",
            "auth_id": "join1-admin",
            "intent": "removed"
        }

        mgr.remote = Mock(return_value=Mock(return_value=_res))
        mgr.remote.return_value.one.return_value.to_simplified = Mock(return_value=_res)
        self._delete(f'{self._endpoint}/join1-admin')
        self.assertStatus(204)
        mgr.remote.assert_called_once_with('smb', 'apply_resources', json.dumps(_res_simplified))


class SMBUsersgroupsTest(ControllerTestCase):
    _endpoint = '/api/smb/usersgroups'

    _usersgroups = {
        "resources": [
            {
                "resource_type": "ceph.smb.usersgroups",
                "users_groups_id": "u2",
                "intent": "present",
                "values": {
                    "users": [
                        {
                            "name": "user3",
                            "password": "pass"
                        }
                    ],
                    "groups": []
                }
            },
            {
                "resource_type": "ceph.smb.usersgroups",
                "users_groups_id": "u1",
                "intent": "present",
                "values": {
                    "users": [
                        {
                            "name": "user2",
                            "password": "pass"
                        }
                    ],
                    "groups": []
                }
            }
        ]
    }

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([SMBUsersgroups])

    def test_list_one_usersgroups(self):
        mgr.remote = Mock(return_value=self._usersgroups['resources'][0])

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody([self._usersgroups['resources'][0]])

    def test_list_multiple_usersgroups(self):
        mgr.remote = Mock(return_value=self._usersgroups)

        self._get(self._endpoint)
        self.assertStatus(200)
        self.assertJsonBody(self._usersgroups['resources'])

    def test_create_usersgroups(self):
        mock_simplified = Mock()
        mock_simplified.to_simplified.return_value = json.dumps(self._usersgroups['resources'][0])
        mgr.remote = Mock(return_value=mock_simplified)

        _usersgroups_data = {'usersgroups': self._usersgroups['resources'][0]}

        self._post(self._endpoint, _usersgroups_data)
        self.assertStatus(201)
        self.assertInJsonBody(json.dumps(self._usersgroups['resources'][0]))

    def test_delete(self):
        _res = {
            "resource_type": "ceph.smb.usersgroups",
            "users_groups_id": "ug1",
            "intent": "removed",
            "auth": {
                "username": "Administrator",
                "password": "Passw0rd"
            }
        }
        _res_simplified = {
            "resource_type": "ceph.smb.usersgroups",
            "users_groups_id": "ug1",
            "intent": "removed"
        }

        mgr.remote = Mock(return_value=Mock(return_value=_res))
        mgr.remote.return_value.one.return_value.to_simplified = Mock(return_value=_res)
        self._delete(f'{self._endpoint}/ug1')
        self.assertStatus(204)
        mgr.remote.assert_called_once_with('smb', 'apply_resources', json.dumps(_res_simplified))
