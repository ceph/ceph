
# -*- coding: utf-8 -*-
import json
import logging
from functools import wraps
from typing import List

from smb.enums import Intent
from smb.proto import Simplified
from smb.resources import Cluster, JoinAuth, Share, UsersAndGroups

from dashboard.controllers._docs import EndpointDoc
from dashboard.controllers._permissions import CreatePermission, DeletePermission
from dashboard.exceptions import DashboardException

from .. import mgr
from ..security import Scope
from . import APIDoc, APIRouter, Endpoint, ReadPermission, RESTController, UIRouter

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
    "public_addrs": ([{
        "address": (str, "This address will be assigned to one of the host's network devices"),
        "destination": (str, "Defines where the system will assign the managed IPs.")
    }], "Public Address"),
    "placement": ({
        "count": (int, "Number of instances to place")
    }, "Placement configuration for the resource")
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
        "provider": (str, "Provider of the CephFS share, e.g., 'samba-vfs'"),
        "subvolumegroup": (str, "Subvolume Group in CephFS file system"),
        "subvolume": (str, "Subvolume within the CephFS file system"),
    }, "Configuration for the CephFS share")
}

JOIN_AUTH_SCHEMA = {
    "resource_type": (str, "ceph.smb.join.auth"),
    "auth_id": (str, "Unique identifier for the join auth resource"),
    "intent": (str, "Desired state of the resource, e.g., 'present' or 'removed'"),
    "auth": ({
        "username": (str, "Username for authentication"),
        "password": (str, "Password for authentication")
    }, "Authentication credentials"),
    "linked_to_cluster": (str, "Optional string containing a cluster ID. \
    If set, the resource is linked to the cluster and will be automatically removed \
    when the cluster is removed")
}

LIST_JOIN_AUTH_SCHEMA = [JOIN_AUTH_SCHEMA]

USERSGROUPS_SCHEMA = {
    "resource_type": (str, "ceph.smb.usersgroups"),
    "users_groups_id": (str, "A short string identifying the usersgroups resource"),
    "intent": (str, "Desired state of the resource, e.g., 'present' or 'removed'"),
    "values": ({
        "users": ([{
            "name": (str, "The user name"),
            "password": (str, "The password for the user")
        }], "List of user objects, each containing a name and password"),
        "groups": ([{
            "name": (str, "The name of the group")
        }], "List of group objects, each containing a name")
    }, "Required object containing users and groups information"),
    "linked_to_cluster": (str, "Optional string containing a cluster ID. \
    If set, the resource is linked to the cluster and will be automatically removed \
    when the cluster is removed")
}

LIST_USERSGROUPS_SCHEMA = [USERSGROUPS_SCHEMA]


def add_results_to_schema(schema):

    results_field = {
        "results": ([{
            "resource": (schema, "Resource"),
            "state": (str, "The current state of the resource,\
                        e.g., 'created', 'updated', 'deleted'"),
            "success": (bool, "Indicates if the operation was successful"),
        }], "List of operation results"),
        "success": (bool, "Indicates if the overall operation was successful")
    }

    return results_field


CLUSTER_SCHEMA_RESULTS = add_results_to_schema(CLUSTER_SCHEMA)
SHARE_SCHEMA_RESULTS = add_results_to_schema(SHARE_SCHEMA)
JOIN_AUTH_SCHEMA_RESULTS = add_results_to_schema(JOIN_AUTH_SCHEMA)
USERSGROUPS_SCHEMA_RESULTS = add_results_to_schema(JOIN_AUTH_SCHEMA_RESULTS)


def raise_on_failure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)

        if isinstance(result, dict) and result.get('success') is False:
            msg = 'Operation failed'

            # Extracts the result msg from either of two possible response structures:
            if 'results' in result:
                msg = result['results'][0].get('msg', msg)
            elif 'msg' in result:
                msg = result['msg']
            raise DashboardException(msg=msg, component='smb')

        return result

    return wrapper


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

    @raise_on_failure
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

    @DeletePermission
    @EndpointDoc("Remove an smb cluster",
                 parameters={
                     'cluster_id': (str, 'Unique identifier for the cluster')},
                 responses={204: None})
    def delete(self, cluster_id: str):
        """
        Remove an smb cluster

        :param cluster_id: Cluster identifier
        :return: None.
        """
        resource = {}
        resource['resource_type'] = self._resource
        resource['cluster_id'] = cluster_id
        resource['intent'] = Intent.REMOVED
        return mgr.remote('smb', 'apply_resources', json.dumps(resource)).one().to_simplified()


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
        return res['resources'] if 'resources' in res else [res]

    @raise_on_failure
    @CreatePermission
    @EndpointDoc("Create smb share",
                 parameters={
                     'share_resource': (str, 'share_resource')
                 },
                 responses={201: SHARE_SCHEMA_RESULTS})
    def create(self, share_resource: Share) -> Simplified:
        """
        Create an smb share

        :param share_resource: Dict share data
        :return: Returns share resource.
        :rtype: Dict[str, Any]
        """
        try:
            return mgr.remote(
                'smb',
                'apply_resources',
                json.dumps(share_resource)).to_simplified()
        except RuntimeError as e:
            raise DashboardException(e, component='smb')

    @ReadPermission
    @EndpointDoc("Get an smb share",
                 parameters={
                     'cluster_id': (str, 'Unique identifier for the cluster'),
                     'share_id': (str, 'Unique identifier for the share')
                 },
                 responses={200: SHARE_SCHEMA})
    def get(self, cluster_id: str, share_id: str) -> Share:
        """
        Get an smb share by cluster and share id
        """
        return mgr.remote('smb', 'show', [f'{self._resource}.{cluster_id}.{share_id}'])

    @raise_on_failure
    @DeletePermission
    @EndpointDoc("Remove an smb share",
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


@APIRouter('/smb/joinauth', Scope.SMB)
@APIDoc("SMB Join Auth API", "SMB")
class SMBJoinAuth(RESTController):
    _resource: str = 'ceph.smb.join.auth'

    @ReadPermission
    @EndpointDoc("List smb join authorization resources",
                 responses={200: LIST_JOIN_AUTH_SCHEMA})
    def list(self) -> List[JoinAuth]:
        """
        List all smb join auth resources

        :return: Returns list of join auth.
        :rtype: List[Dict]
        """
        res = mgr.remote(
            'smb',
            'show',
            [self._resource])
        return res['resources'] if 'resources' in res else [res]

    @ReadPermission
    @EndpointDoc("Get smb join authorization resource",
                 responses={200: JOIN_AUTH_SCHEMA})
    def get(self, auth_id: str) -> JoinAuth:
        """
        Get Join auth resource

        :return: Returns join auth.
        :rtype: Dict
        """
        res = mgr.remote(
            'smb',
            'show',
            [f'{self._resource}.{auth_id}'])
        return res['resources'] if 'resources' in res else res

    @CreatePermission
    @EndpointDoc("Create smb join auth",
                 parameters={
                     'auth_id': (str, 'auth_id'),
                     'username': (str, 'username'),
                     'password': (str, 'password')
                 },
                 responses={201: JOIN_AUTH_SCHEMA_RESULTS})
    def create(self, join_auth: JoinAuth) -> Simplified:
        """
        Create smb join auth resource

        :return: Returns join auth resource.
        :rtype: Dict
        """
        return mgr.remote('smb', 'apply_resources', json.dumps(join_auth)).to_simplified()

    @CreatePermission
    @EndpointDoc("Delete smb join auth",
                 parameters={
                     'auth_id': (str, 'auth_id')
                 },
                 responses={204: None})
    def delete(self, auth_id: str) -> None:
        """
        Delete smb join auth resource

        :param auth_id: Join Auth identifier
        :return: None.
        """
        resource = {}
        resource['resource_type'] = self._resource
        resource['auth_id'] = auth_id
        resource['intent'] = Intent.REMOVED
        return mgr.remote('smb', 'apply_resources', json.dumps(resource)).one().to_simplified()


@APIRouter('/smb/usersgroups', Scope.SMB)
@APIDoc("SMB Users Groups API", "SMB")
class SMBUsersgroups(RESTController):
    _resource: str = 'ceph.smb.usersgroups'

    @ReadPermission
    @EndpointDoc("List smb user resources",
                 responses={200: LIST_USERSGROUPS_SCHEMA})
    def list(self) -> List[UsersAndGroups]:
        """
        List all smb usersgroups resources

        :return: Returns list of usersgroups
        :rtype: List[Dict]
        """
        res = mgr.remote(
            'smb',
            'show',
            [self._resource])
        return res['resources'] if 'resources' in res else [res]

    @ReadPermission
    @EndpointDoc("Get smb usersgroups authorization resource",
                 responses={200: USERSGROUPS_SCHEMA})
    def get(self, users_groups_id: str) -> UsersAndGroups:
        """
        Get Users and groups resource

        :return: Returns join auth.
        :rtype: Dict
        """
        res = mgr.remote(
            'smb',
            'show',
            [f'{self._resource}.{users_groups_id}'])
        return res['resources'] if 'resources' in res else res

    @CreatePermission
    @EndpointDoc("Create smb usersgroups",
                 parameters={
                     'users_groups_id': (str, 'users_groups_id'),
                     'username': (str, 'username'),
                     'password': (str, 'password')
                 },
                 responses={201: USERSGROUPS_SCHEMA_RESULTS})
    def create(self, usersgroups: UsersAndGroups) -> Simplified:
        """
        Create smb usersgroups resource

        :return: Returns usersgroups resource.
        :rtype: Dict
        """
        return mgr.remote('smb', 'apply_resources', json.dumps(usersgroups)).to_simplified()

    @CreatePermission
    @EndpointDoc("Delete smb join auth",
                 parameters={
                     'users_groups_id': (str, 'users_groups_id')
                 },
                 responses={204: None})
    def delete(self, users_groups_id: str) -> None:
        """
        Delete smb usersgroups resource

        :param users_group_id: Users  identifier
        :return: None.
        """
        resource = {}
        resource['resource_type'] = self._resource
        resource['users_groups_id'] = users_groups_id
        resource['intent'] = Intent.REMOVED
        return mgr.remote('smb', 'apply_resources', json.dumps(resource)).one().to_simplified()


@UIRouter('/smb')
class SMBStatus(RESTController):
    @EndpointDoc("Get SMB feature status")
    @Endpoint()
    @ReadPermission
    def status(self):
        status = {'available': False, 'message': None}
        try:
            mgr.remote('smb', 'show', ['ceph.smb.cluster'])
            status['available'] = True
        except (ImportError, RuntimeError):
            status['message'] = 'SMB module is not enabled'
        return status
