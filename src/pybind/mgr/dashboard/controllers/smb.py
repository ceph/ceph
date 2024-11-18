
# -*- coding: utf-8 -*-

import json
import logging
from typing import List

from smb.enums import Intent
from smb.proto import Simplified
from smb.resources import Cluster, Share

from dashboard.controllers._docs import EndpointDoc
from dashboard.controllers._permissions import CreatePermission, DeletePermission
from dashboard.exceptions import DashboardException

from .. import mgr
from ..security import Scope
from . import APIDoc, APIRouter, ReadPermission, RESTController

logger = logging.getLogger('controllers.smb')

CLUSTER_SCHEMA = {
    "resource_type": (str, "ceph.smb.cluster"),
    "cluster_id": (str, "Unique identifier for the cluster"),
    "auth_mode": (str, "Either 'active-directory' or 'user'"),
    "intent": (str, "Desired state of the resource, e.g., 'present' or 'removed'"),
    "domain_settings": ({
        "realm": (str, "Domain realm, e.g., 'DOMAIN1.SINK.TEST'"),
        "join_sources": ([{
            "source_type": (str, "resource"),
            "ref": (str, "Reference identifier for the join auth resource")
        }], "List of join auth sources for domain settings")
    }, "Domain-specific settings for active-directory auth mode"),
    "user_group_settings": ([{
        "source_type": (str, "resource"),
        "ref": (str, "Reference identifier for the user group resource")
    }], "User group settings for user auth mode"),
    "custom_dns": ([str], "List of custom DNS server addresses"),
    "placement": ({
        "count": (int, "Number of instances to place")
    }, "Placement configuration for the resource")
}

CLUSTER_SCHEMA_RESULTS = {
    "results": ([{
        "resource": ({
            "resource_type": (str, "ceph.smb.cluster"),
            "cluster_id": (str, "Unique identifier for the cluster"),
            "auth_mode": (str, "Either 'active-directory' or 'user'"),
            "intent": (str, "Desired state of the resource, e.g., 'present' or 'removed'"),
            "domain_settings": ({
                "realm": (str, "Domain realm, e.g., 'DOMAIN1.SINK.TEST'"),
                "join_sources": ([{
                    "source_type": (str, "resource"),
                    "ref": (str, "Reference identifier for the join auth resource")
                }], "List of join auth sources for domain settings")
            }, "Domain-specific settings for active-directory auth mode"),
            "user_group_settings": ([{
                "source_type": (str, "resource"),
                "ref": (str, "Reference identifier for the user group resource")
            }], "User group settings for user auth mode (optional)"),
            "custom_dns": ([str], "List of custom DNS server addresses (optional)"),
            "placement": ({
                "count": (int, "Number of instances to place")
            }, "Placement configuration for the resource (optional)"),
        }, "Resource details"),
        "state": (str, "State of the resource"),
        "success": (bool, "Indicates whether the operation was successful")
    }], "List of results with resource details"),
    "success": (bool, "Overall success status of the operation")
}

LIST_CLUSTER_SCHEMA = [CLUSTER_SCHEMA]

SHARE_SCHEMA = {
    "resource_type": (str, "ceph.smb.share"),
    "cluster_id": (str, "Unique identifier for the cluster"),
    "share_id": (str, "Unique identifier for the share"),
    "intent": (str, "Desired state of the resource, e.g., 'present' or 'removed'"),
    "name": (str, "Name of the share"),
    "readonly": (bool, "Indicates if the share is read-only"),
    "browseable": (bool, "Indicates if the share is browseable"),
    "cephfs": ({
        "volume": (str, "Name of the CephFS file system"),
        "path": (str, "Path within the CephFS file system"),
        "provider": (str, "Provider of the CephFS share, e.g., 'samba-vfs'")
    }, "Configuration for the CephFS share")
}


@APIRouter('/smb/cluster', Scope.SMB)
@APIDoc("SMB Cluster Management API", "SMB")
class SMBCluster(RESTController):
    _resource: str = 'ceph.smb.cluster'

    @ReadPermission
    @EndpointDoc("List smb clusters",
                 responses={200: LIST_CLUSTER_SCHEMA})
    def list(self) -> List[Cluster]:
        """
        List smb clusters
        """
        res = mgr.remote('smb', 'show', [self._resource])
        return res['resources'] if 'resources' in res else [res]

    @ReadPermission
    @EndpointDoc("Get an smb cluster",
                 parameters={
                     'cluster_id': (str, 'Unique identifier for the cluster')
                 },
                 responses={200: CLUSTER_SCHEMA})
    def get(self, cluster_id: str) -> Cluster:
        """
        Get an smb cluster by cluster id
        """
        return mgr.remote('smb', 'show', [f'{self._resource}.{cluster_id}'])

    @CreatePermission
    @EndpointDoc("Create smb cluster",
                 parameters={
                     'cluster_resource': (str, 'cluster_resource')
                 },
                 responses={201: CLUSTER_SCHEMA_RESULTS})
    def create(self, cluster_resource: Cluster) -> Simplified:
        """
        Create an smb cluster

        :param cluster_resource: Dict cluster data
        :return: Returns cluster resource.
        :rtype: Dict[str, Any]
        """
        try:
            return mgr.remote(
                'smb',
                'apply_resources',
                json.dumps(cluster_resource)).to_simplified()
        except RuntimeError as e:
            raise DashboardException(e, component='smb')


@APIRouter('/smb/share', Scope.SMB)
@APIDoc("SMB Share Management API", "SMB")
class SMBShare(RESTController):
    _resource: str = 'ceph.smb.share'

    @ReadPermission
    @EndpointDoc("List smb shares",
                 parameters={
                     'cluster_id': (str, 'Unique identifier for the cluster')
                 },
                 responses={200: SHARE_SCHEMA})
    def list(self, cluster_id: str = '') -> List[Share]:
        """
        List all smb shares or all shares for a given cluster

        :param cluster_id: Dict containing cluster information
        :return: Returns list of shares.
        :rtype: List[Dict]
        """
        res = mgr.remote(
            'smb',
            'show',
            [f'{self._resource}.{cluster_id}' if cluster_id else self._resource])
        return res['resources'] if 'resources' in res else res

    @DeletePermission
    @EndpointDoc("Remove smb shares",
                 parameters={
                     'cluster_id': (str, 'Unique identifier for the cluster'),
                     'share_id': (str, 'Unique identifier for the share')
                 },
                 responses={204: None})
    def delete(self, cluster_id: str, share_id: str):
        """
        Remove an smb share from a given cluster

        :param cluster_id: Cluster identifier
        :param share_id: Share identifier
        :return: None.
        """
        resource = {}
        resource['resource_type'] = self._resource
        resource['cluster_id'] = cluster_id
        resource['share_id'] = share_id
        resource['intent'] = Intent.REMOVED
        return mgr.remote('smb', 'apply_resources', json.dumps(resource)).one().to_simplified()
