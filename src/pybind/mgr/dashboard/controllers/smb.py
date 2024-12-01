
# -*- coding: utf-8 -*-

import logging
import json

from typing import Dict, List, Union


from dashboard.controllers._permissions import CreatePermission, DeletePermission
from smb import results
from smb.enums import AuthMode, SMBClustering, Intent

from .. import mgr
from ..security import Scope
from . import APIDoc, APIRouter, ReadPermission, RESTController

logger = logging.getLogger('controllers.smb')


@APIRouter('/smb/cluster', Scope.SMB)
@APIDoc("SMB Cluster Management API", "SMB")
class SMBCluster(RESTController):
    _resource: str = 'ceph.smb.cluster'

    @ReadPermission
    def list(self) -> List[Dict]:
        res = mgr.remote('smb', 'show', [self._resource])
        if isinstance(res, list):
            return res
        return [res]

    @CreatePermission
    def create(
        self,
        cluster_id: str,
        auth_mode: AuthMode,
        intent: Intent = Intent.PRESENT,
        domain_settings: Dict = {},
        user_group_settings: List[str] = [],
        custom_dns: List[str] = [],
        placement: str = '',
        clustering: Union[SMBClustering, None] = None,
        public_addrs: List[str] = [],
        custom_smb_global_options: Dict = {}
    ) -> results.Simplified:
        # pylint: disable=unused-argument
        cluster_data = locals().items()

        resource = {}
        resource['resource_type'] = self._resource

        for key, value in cluster_data:
            if key != 'self' and value:
                if key == 'domain_settings' and domain_settings:
                    if 'domain_settings' not in resource:
                        resource[key] = {}
                        resource[key]['join_sources'] = []

                    resource[key]['realm'] = domain_settings['realm'] if 'realm' in domain_settings else None

                    for auth_id in domain_settings['join_sources'] if 'join_sources' in domain_settings else []:
                        value = {'source_type': 'resource', 'ref': auth_id}
                        resource[key]['join_sources'].append(value)

                elif key == 'user_group_settings' and user_group_settings:
                    if 'user_group_settings' not in resource:
                            resource[key] = []

                    for user_id in user_group_settings:
                        value = {'source_type': 'resource', 'ref': user_id}
                        resource[key].append(value)
                else:
                    resource[key] = value
        res =  mgr.remote('smb', 'apply_resources', json.dumps(resource))
        return res.to_simplified() if type(res) is results.Result else res

    
    @DeletePermission
    def delete(self, cluster_id: str) -> results.Simplified:
        resource = {
            'resource_type': self._resource,
            'cluster_id': cluster_id
        }

        res = mgr.remote('smb', 'delete_cluster', json.dumps(resource))
        return res.to_simplified() if isinstance(res, results.Result) else res

@APIRouter('/smb/share', Scope.SMB)
@APIDoc("SMB Share Management API", "SMB")
class SMBShare(RESTController):
    _resource = 'ceph.smb.share'

    @ReadPermission
    def list(self, cluster_id: str = '') -> List[Dict]:
            return mgr.remote('smb', 'show', [f'{self._resource}.{cluster_id}' if cluster_id else f'{self._resource}'])

    @DeletePermission
    def delete(self, cluster_id: str, share_id: str) -> results.Simplified:
        resource = {}
        resource['resource_type'] = self._resource
        resource['cluster_id'] = cluster_id
        resource['share_id'] = share_id
        resource['intent'] = Intent.REMOVED
        return mgr.remote('smb', 'apply_resources', json.dumps(resource)).to_simplified()

