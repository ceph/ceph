# -*- coding: utf-8 -*-

# pylint: disable=C0302
import json
import logging
import re
from typing import Any, Dict, List, NamedTuple, Optional, Union

import cherrypy

from .. import mgr
from ..exceptions import DashboardException
from ..rest_client import RequestException
from ..security import Permission, Scope
from ..services.auth import AuthManager, JwtManager
from ..services.ceph_service import CephService
from ..services.rgw_client import _SYNC_GROUP_ID, NoRgwDaemonsException, RgwClient, RgwMultisite
from ..tools import json_str_to_object, str_to_bool
from . import APIDoc, APIRouter, BaseController, CreatePermission, \
    CRUDCollectionMethod, CRUDEndpoint, DeletePermission, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, UIRouter, UpdatePermission, \
    allow_empty_body
from ._crud import CRUDMeta, Form, FormField, FormTaskInfo, Icon, MethodType, \
    TableAction, Validator, VerticalContainer
from ._version import APIVersion

logger = logging.getLogger("controllers.rgw")

RGW_SCHEMA = {
    "available": (bool, "Is RGW available?"),
    "message": (str, "Descriptions")
}

RGW_DAEMON_SCHEMA = {
    "id": (str, "Daemon ID"),
    "version": (str, "Ceph Version"),
    "server_hostname": (str, ""),
    "zonegroup_name": (str, "Zone Group"),
    "zone_name": (str, "Zone"),
    "port": (int, "Port"),
}

RGW_USER_SCHEMA = {
    "list_of_users": ([str], "list of rgw users")
}


@UIRouter('/rgw', Scope.RGW)
@APIDoc("RGW Management API", "Rgw")
class Rgw(BaseController):
    @Endpoint()
    @ReadPermission
    @EndpointDoc("Display RGW Status",
                 responses={200: RGW_SCHEMA})
    def status(self) -> dict:
        status = {'available': False, 'message': None}
        try:
            instance = RgwClient.admin_instance()
            # Check if the service is online.
            try:
                is_online = instance.is_service_online()
            except RequestException as e:
                # Drop this instance because the RGW client seems not to
                # exist anymore (maybe removed via orchestrator). Removing
                # the instance from the cache will result in the correct
                # error message next time when the backend tries to
                # establish a new connection (-> 'No RGW found' instead
                # of 'RGW REST API failed request ...').
                # Note, this only applies to auto-detected RGW clients.
                RgwClient.drop_instance(instance)
                raise e
            if not is_online:
                msg = 'Failed to connect to the Object Gateway\'s Admin Ops API.'
                raise RequestException(msg)
            # Ensure the system flag is set for the API user ID.
            if not instance.is_system_user():  # pragma: no cover - no complexity there
                msg = 'The system flag is not set for user "{}".'.format(
                    instance.userid)
                raise RequestException(msg)
            status['available'] = True
        except (DashboardException, RequestException, NoRgwDaemonsException) as ex:
            status['message'] = str(ex)  # type: ignore
        return status


@UIRouter('/rgw/multisite')
class RgwMultisiteStatus(RESTController):
    @Endpoint()
    @ReadPermission
    @EndpointDoc("Get the multisite sync status")
    # pylint: disable=R0801
    def status(self):
        status = {'available': True, 'message': None}
        multisite_instance = RgwMultisite()
        is_multisite_configured = multisite_instance.get_multisite_status()
        if not is_multisite_configured:
            status['available'] = False
            status['message'] = 'Multi-site provides disaster recovery and may also \
                serve as a foundation for content delivery networks'  # type: ignore
        return status

    @RESTController.Collection(method='PUT', path='/migrate')
    @allow_empty_body
    # pylint: disable=W0102,W0613
    def migrate(self, daemon_name=None, realm_name=None, zonegroup_name=None, zone_name=None,
                zonegroup_endpoints=None, zone_endpoints=None, access_key=None,
                secret_key=None):
        multisite_instance = RgwMultisite()
        result = multisite_instance.migrate_to_multisite(realm_name, zonegroup_name,
                                                         zone_name, zonegroup_endpoints,
                                                         zone_endpoints, access_key,
                                                         secret_key)
        return result


@APIRouter('rgw/multisite', Scope.RGW)
@APIDoc("RGW Multisite Management API", "RgwMultisite")
class RgwMultisiteController(RESTController):
    @Endpoint(path='/sync_status')
    @EndpointDoc("Get the sync status")
    @ReadPermission
    @allow_empty_body
    # pylint: disable=W0102,W0613
    def get_sync_status(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_multisite_sync_status()
        return result

    @Endpoint(path='/sync-policy')
    @EndpointDoc("Get the sync policy")
    @ReadPermission
    def get_sync_policy(self, bucket_name='', zonegroup_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.get_sync_policy(bucket_name, zonegroup_name)

    @Endpoint(path='/sync-policy-group')
    @EndpointDoc("Get the sync policy group")
    @ReadPermission
    def get_sync_policy_group(self, group_id: str, bucket_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.get_sync_policy_group(group_id, bucket_name)

    @Endpoint(method='POST', path='/sync-policy-group')
    @EndpointDoc("Create the sync policy group")
    @CreatePermission
    def create_sync_policy_group(self, group_id: str, status: str, bucket_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.create_sync_policy_group(group_id, status, bucket_name)

    @Endpoint(method='PUT', path='/sync-policy-group')
    @EndpointDoc("Update the sync policy group")
    @UpdatePermission
    def update_sync_policy_group(self, group_id: str, status: str, bucket_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.update_sync_policy_group(group_id, status, bucket_name)

    @Endpoint(method='DELETE', path='/sync-policy-group')
    @EndpointDoc("Remove the sync policy group")
    @DeletePermission
    def remove_sync_policy_group(self, group_id: str, bucket_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.remove_sync_policy_group(group_id, bucket_name)

    @Endpoint(method='PUT', path='/sync-flow')
    @EndpointDoc("Create or update the sync flow")
    @CreatePermission
    def create_sync_flow(self, flow_id: str, flow_type: str, group_id: str,
                         source_zone='', destination_zone='', zones: Optional[List[str]] = None,
                         bucket_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.create_sync_flow(group_id, flow_id, flow_type, zones,
                                                   bucket_name, source_zone, destination_zone)

    @Endpoint(method='DELETE', path='/sync-flow')
    @EndpointDoc("Remove the sync flow")
    @DeletePermission
    def remove_sync_flow(self, flow_id: str, flow_type: str, group_id: str,
                         source_zone='', destination_zone='', zones: Optional[List[str]] = None,
                         bucket_name=''):
        multisite_instance = RgwMultisite()
        return multisite_instance.remove_sync_flow(group_id, flow_id, flow_type, source_zone,
                                                   destination_zone, zones, bucket_name)

    @Endpoint(method='PUT', path='/sync-pipe')
    @EndpointDoc("Create or update the sync pipe")
    @CreatePermission
    def create_sync_pipe(self, group_id: str, pipe_id: str,
                         source_zones: Optional[List[str]] = None,
                         destination_zones: Optional[List[str]] = None,
                         destination_buckets: Optional[List[str]] = None,
                         bucket_name: str = ''):
        multisite_instance = RgwMultisite()
        return multisite_instance.create_sync_pipe(group_id, pipe_id, source_zones,
                                                   destination_zones, destination_buckets,
                                                   bucket_name)

    @Endpoint(method='DELETE', path='/sync-pipe')
    @EndpointDoc("Remove the sync pipe")
    @DeletePermission
    def remove_sync_pipe(self, group_id: str, pipe_id: str,
                         source_zones: Optional[List[str]] = None,
                         destination_zones: Optional[List[str]] = None,
                         destination_buckets: Optional[List[str]] = None,
                         bucket_name: str = ''):
        multisite_instance = RgwMultisite()
        return multisite_instance.remove_sync_pipe(group_id, pipe_id, source_zones,
                                                   destination_zones, destination_buckets,
                                                   bucket_name)


@APIRouter('/rgw/daemon', Scope.RGW)
@APIDoc("RGW Daemon Management API", "RgwDaemon")
class RgwDaemon(RESTController):
    @EndpointDoc("Display RGW Daemons",
                 responses={200: [RGW_DAEMON_SCHEMA]})
    def list(self) -> List[dict]:
        daemons: List[dict] = []
        try:
            instance = RgwClient.admin_instance()
        except NoRgwDaemonsException:
            return daemons

        for hostname, server in CephService.get_service_map('rgw').items():
            for service in server['services']:
                metadata = service['metadata']

                frontend_config = metadata['frontend_config#0']
                port_match = re.search(r"port=(\d+)", frontend_config)
                port = None
                if port_match:
                    port = port_match.group(1)
                else:
                    match_from_endpoint = re.search(r"endpoint=\S+:(\d+)", frontend_config)
                    if match_from_endpoint:
                        port = match_from_endpoint.group(1)

                # extract per-daemon service data and health
                daemon = {
                    'id': metadata['id'],
                    'service_map_id': service['id'],
                    'version': metadata['ceph_version'],
                    'server_hostname': hostname,
                    'realm_name': metadata['realm_name'],
                    'zonegroup_name': metadata['zonegroup_name'],
                    'zonegroup_id': metadata['zonegroup_id'],
                    'zone_name': metadata['zone_name'],
                    'default': instance.daemon.name == metadata['id'],
                    'port': int(port) if port else None
                }

                daemons.append(daemon)

        return sorted(daemons, key=lambda k: k['id'])

    def get(self, svc_id):
        # type: (str) -> dict
        daemon = {
            'rgw_metadata': [],
            'rgw_id': svc_id,
            'rgw_status': []
        }
        service = CephService.get_service('rgw', svc_id)
        if not service:
            raise cherrypy.NotFound('Service rgw {} is not available'.format(svc_id))

        metadata = service['metadata']
        status = service['status']
        if 'json' in status:
            try:
                status = json.loads(status['json'])
            except ValueError:
                logger.warning('%s had invalid status json', service['id'])
                status = {}
        else:
            logger.warning('%s has no key "json" in status', service['id'])

        daemon['rgw_metadata'] = metadata
        daemon['rgw_status'] = status
        return daemon

    @RESTController.Collection(method='PUT', path='/set_multisite_config')
    @allow_empty_body
    def set_multisite_config(self, realm_name=None, zonegroup_name=None,
                             zone_name=None, daemon_name=None):
        CephService.set_multisite_config(realm_name, zonegroup_name, zone_name, daemon_name)


class RgwRESTController(RESTController):
    def proxy(self, daemon_name, method, path, params=None, json_response=True):
        try:
            instance = RgwClient.admin_instance(daemon_name=daemon_name)
            result = instance.proxy(method, path, params, None)
            if json_response:
                result = json_str_to_object(result)
            return result
        except (DashboardException, RequestException) as e:
            http_status_code = e.status if isinstance(e, DashboardException) else 500
            raise DashboardException(e, http_status_code=http_status_code, component='rgw')


@APIRouter('/rgw/site', Scope.RGW)
@APIDoc("RGW Site Management API", "RgwSite")
class RgwSite(RgwRESTController):
    def list(self, query=None, daemon_name=None):
        if query == 'placement-targets':
            return RgwClient.admin_instance(daemon_name=daemon_name).get_placement_targets()
        if query == 'realms':
            return RgwClient.admin_instance(daemon_name=daemon_name).get_realms()
        if query == 'default-realm':
            return RgwClient.admin_instance(daemon_name=daemon_name).get_default_realm()
        if query == 'default-zonegroup':
            return RgwMultisite().get_all_zonegroups_info()['default_zonegroup']

        # @TODO: for multisite: by default, retrieve cluster topology/map.
        raise DashboardException(http_status_code=501, component='rgw', msg='Not Implemented')


@APIRouter('/rgw/bucket', Scope.RGW)
@APIDoc("RGW Bucket Management API", "RgwBucket")
class RgwBucket(RgwRESTController):
    def _append_bid(self, bucket):
        """
        Append the bucket identifier that looks like [<tenant>/]<bucket>.
        See http://docs.ceph.com/docs/nautilus/radosgw/multitenancy/ for
        more information.
        :param bucket: The bucket parameters.
        :type bucket: dict
        :return: The modified bucket parameters including the 'bid' parameter.
        :rtype: dict
        """
        if isinstance(bucket, dict):
            bucket['bid'] = '{}/{}'.format(bucket['tenant'], bucket['bucket']) \
                if bucket['tenant'] else bucket['bucket']
        return bucket

    def _get_versioning(self, owner, daemon_name, bucket_name):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.get_bucket_versioning(bucket_name)

    def _set_versioning(self, owner, daemon_name, bucket_name, versioning_state, mfa_delete,
                        mfa_token_serial, mfa_token_pin):
        bucket_versioning = self._get_versioning(owner, daemon_name, bucket_name)
        if versioning_state != bucket_versioning['Status']\
                or (mfa_delete and mfa_delete != bucket_versioning['MfaDelete']):
            rgw_client = RgwClient.instance(owner, daemon_name)
            rgw_client.set_bucket_versioning(bucket_name, versioning_state, mfa_delete,
                                             mfa_token_serial, mfa_token_pin)

    def _set_encryption(self, bid, encryption_type, key_id, daemon_name, owner):

        rgw_client = RgwClient.instance(owner, daemon_name)
        rgw_client.set_bucket_encryption(bid, key_id, encryption_type)

    # pylint: disable=W0613
    def _set_encryption_config(self, encryption_type, kms_provider, auth_method, secret_engine,
                               secret_path, namespace, address, token, daemon_name, owner,
                               ssl_cert, client_cert, client_key):

        CephService.set_encryption_config(encryption_type, kms_provider, auth_method,
                                          secret_engine, secret_path, namespace, address,
                                          token, daemon_name, ssl_cert, client_cert, client_key)

    def _get_encryption(self, bucket_name, daemon_name, owner):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.get_bucket_encryption(bucket_name)

    def _delete_encryption(self, bucket_name, daemon_name, owner):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.delete_bucket_encryption(bucket_name)

    def _get_locking(self, owner, daemon_name, bucket_name):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.get_bucket_locking(bucket_name)

    def _set_locking(self, owner, daemon_name, bucket_name, mode,
                     retention_period_days, retention_period_years):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.set_bucket_locking(bucket_name, mode,
                                             retention_period_days,
                                             retention_period_years)

    def _get_policy(self, bucket: str, daemon_name, owner):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.get_bucket_policy(bucket)

    def _set_policy(self, bucket_name: str, policy: str, daemon_name, owner):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.set_bucket_policy(bucket_name, policy)

    def _set_tags(self, bucket_name, tags, daemon_name, owner):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.set_tags(bucket_name, tags)

    def _get_acl(self, bucket_name, daemon_name, owner):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return str(rgw_client.get_acl(bucket_name))

    def _set_acl(self, bucket_name: str, acl: str, owner, daemon_name):
        rgw_client = RgwClient.instance(owner, daemon_name)
        return rgw_client.set_acl(bucket_name, acl)

    def _set_replication(self, bucket_name: str, replication: bool, owner, daemon_name):
        multisite = RgwMultisite()
        rgw_client = RgwClient.instance(owner, daemon_name)
        zonegroup_name = RgwClient.admin_instance(daemon_name=daemon_name).get_default_zonegroup()

        policy_exists = multisite.policy_group_exists(_SYNC_GROUP_ID, zonegroup_name)
        if replication and not policy_exists:
            multisite.create_dashboard_admin_sync_group(zonegroup_name=zonegroup_name)
        return rgw_client.set_bucket_replication(bucket_name, replication)

    @staticmethod
    def strip_tenant_from_bucket_name(bucket_name):
        # type (str) -> str
        """
        >>> RgwBucket.strip_tenant_from_bucket_name('tenant/bucket-name')
        'bucket-name'
        >>> RgwBucket.strip_tenant_from_bucket_name('bucket-name')
        'bucket-name'
        """
        return bucket_name[bucket_name.find('/') + 1:]

    @staticmethod
    def get_s3_bucket_name(bucket_name, tenant=None):
        # type (str, str) -> str
        """
        >>> RgwBucket.get_s3_bucket_name('bucket-name', 'tenant')
        'tenant:bucket-name'
        >>> RgwBucket.get_s3_bucket_name('tenant/bucket-name', 'tenant')
        'tenant:bucket-name'
        >>> RgwBucket.get_s3_bucket_name('bucket-name')
        'bucket-name'
        """
        bucket_name = RgwBucket.strip_tenant_from_bucket_name(bucket_name)
        if tenant:
            bucket_name = '{}:{}'.format(tenant, bucket_name)
        return bucket_name

    @RESTController.MethodMap(version=APIVersion(1, 1))  # type: ignore
    def list(self, stats: bool = False, daemon_name: Optional[str] = None,
             uid: Optional[str] = None) -> List[Union[str, Dict[str, Any]]]:
        query_params = f'?stats={str_to_bool(stats)}'
        if uid and uid.strip():
            query_params = f'{query_params}&uid={uid.strip()}'
        result = self.proxy(daemon_name, 'GET', 'bucket{}'.format(query_params))

        if stats:
            result = [self._append_bid(bucket) for bucket in result]

        return result

    def get(self, bucket, daemon_name=None):
        # type: (str, Optional[str]) -> dict
        result = self.proxy(daemon_name, 'GET', 'bucket', {'bucket': bucket})
        bucket_name = RgwBucket.get_s3_bucket_name(result['bucket'],
                                                   result['tenant'])

        # Append the versioning configuration.
        versioning = self._get_versioning(result['owner'], daemon_name, bucket_name)
        encryption = self._get_encryption(bucket_name, daemon_name, result['owner'])
        result['encryption'] = encryption['Status']
        result['versioning'] = versioning['Status']
        result['mfa_delete'] = versioning['MfaDelete']
        result['bucket_policy'] = self._get_policy(bucket_name, daemon_name, result['owner'])
        result['acl'] = self._get_acl(bucket_name, daemon_name, result['owner'])

        # Append the locking configuration.
        locking = self._get_locking(result['owner'], daemon_name, bucket_name)
        result.update(locking)

        return self._append_bid(result)

    @allow_empty_body
    def create(self, bucket, uid, zonegroup=None, placement_target=None,
               lock_enabled='false', lock_mode=None,
               lock_retention_period_days=None,
               lock_retention_period_years=None, encryption_state='false',
               encryption_type=None, key_id=None, tags=None,
               bucket_policy=None, canned_acl=None, replication='false',
               daemon_name=None):
        lock_enabled = str_to_bool(lock_enabled)
        encryption_state = str_to_bool(encryption_state)
        replication = str_to_bool(replication)
        try:
            rgw_client = RgwClient.instance(uid, daemon_name)
            result = rgw_client.create_bucket(bucket, zonegroup,
                                              placement_target,
                                              lock_enabled)
            if lock_enabled:
                self._set_locking(uid, daemon_name, bucket, lock_mode,
                                  lock_retention_period_days,
                                  lock_retention_period_years)

            if encryption_state:
                self._set_encryption(bucket, encryption_type, key_id, daemon_name, uid)

            if tags:
                self._set_tags(bucket, tags, daemon_name, uid)

            if bucket_policy:
                self._set_policy(bucket, bucket_policy, daemon_name, uid)

            if canned_acl:
                self._set_acl(bucket, canned_acl, uid, daemon_name)

            if replication:
                self._set_replication(bucket, replication, uid, daemon_name)
            return result
        except RequestException as e:  # pragma: no cover - handling is too obvious
            raise DashboardException(e, http_status_code=500, component='rgw')

    @allow_empty_body
    def set(self, bucket, bucket_id, uid, versioning_state=None,
            encryption_state='false', encryption_type=None, key_id=None,
            mfa_delete=None, mfa_token_serial=None, mfa_token_pin=None,
            lock_mode=None, lock_retention_period_days=None,
            lock_retention_period_years=None, tags=None, bucket_policy=None,
            canned_acl=None, daemon_name=None):
        encryption_state = str_to_bool(encryption_state)
        # When linking a non-tenant-user owned bucket to a tenanted user, we
        # need to prefix bucket name with '/'. e.g. photos -> /photos
        if '$' in uid and '/' not in bucket:
            bucket = '/{}'.format(bucket)

        # Link bucket to new user:
        result = self.proxy(daemon_name,
                            'PUT',
                            'bucket', {
                                'bucket': bucket,
                                'bucket-id': bucket_id,
                                'uid': uid
                            },
                            json_response=False)

        uid_tenant = uid[:uid.find('$')] if uid.find('$') >= 0 else None
        bucket_name = RgwBucket.get_s3_bucket_name(bucket, uid_tenant)

        locking = self._get_locking(uid, daemon_name, bucket_name)
        if versioning_state:
            if versioning_state == 'Suspended' and locking['lock_enabled']:
                raise DashboardException(msg='Bucket versioning cannot be disabled/suspended '
                                             'on buckets with object lock enabled ',
                                             http_status_code=409, component='rgw')
            self._set_versioning(uid, daemon_name, bucket_name, versioning_state,
                                 mfa_delete, mfa_token_serial, mfa_token_pin)

        # Update locking if it is enabled.
        if locking['lock_enabled']:
            self._set_locking(uid, daemon_name, bucket_name, lock_mode,
                              lock_retention_period_days,
                              lock_retention_period_years)

        encryption_status = self._get_encryption(bucket_name, daemon_name, uid)
        if encryption_state and encryption_status['Status'] != 'Enabled':
            self._set_encryption(bucket_name, encryption_type, key_id, daemon_name, uid)
        if encryption_status['Status'] == 'Enabled' and (not encryption_state):
            self._delete_encryption(bucket_name, daemon_name, uid)
        if tags:
            self._set_tags(bucket_name, tags, daemon_name, uid)
        if bucket_policy:
            self._set_policy(bucket_name, bucket_policy, daemon_name, uid)
        if canned_acl:
            self._set_acl(bucket_name, canned_acl, uid, daemon_name)
        return self._append_bid(result)

    def delete(self, bucket, purge_objects='true', daemon_name=None):
        return self.proxy(daemon_name, 'DELETE', 'bucket', {
            'bucket': bucket,
            'purge-objects': purge_objects
        }, json_response=False)

    @RESTController.Collection(method='PUT', path='/setEncryptionConfig')
    @allow_empty_body
    def set_encryption_config(self, encryption_type=None, kms_provider=None, auth_method=None,
                              secret_engine=None, secret_path='', namespace='', address=None,
                              token=None, daemon_name=None, owner=None, ssl_cert=None,
                              client_cert=None, client_key=None):
        return self._set_encryption_config(encryption_type, kms_provider, auth_method,
                                           secret_engine, secret_path, namespace,
                                           address, token, daemon_name, owner, ssl_cert,
                                           client_cert, client_key)

    @RESTController.Collection(method='GET', path='/getEncryption')
    @allow_empty_body
    def get_encryption(self, bucket_name, daemon_name=None, owner=None):
        return self._get_encryption(bucket_name, daemon_name, owner)

    @RESTController.Collection(method='DELETE', path='/deleteEncryption')
    @allow_empty_body
    def delete_encryption(self, bucket_name, daemon_name=None, owner=None):
        return self._delete_encryption(bucket_name, daemon_name, owner)

    @RESTController.Collection(method='GET', path='/getEncryptionConfig')
    @allow_empty_body
    def get_encryption_config(self, daemon_name=None, owner=None):
        return CephService.get_encryption_config(daemon_name)


@UIRouter('/rgw/bucket', Scope.RGW)
class RgwBucketUi(RgwBucket):
    @Endpoint('GET')
    @ReadPermission
    # pylint: disable=W0613
    def buckets_and_users_count(self, daemon_name=None):
        buckets_count = 0
        users_count = 0
        daemon_object = RgwDaemon()
        daemons = json.loads(daemon_object.list())
        unique_realms = set()
        for daemon in daemons:
            realm_name = daemon.get('realm_name', None)
            if realm_name:
                if realm_name not in unique_realms:
                    unique_realms.add(realm_name)
                    buckets = json.loads(RgwBucket.list(self, daemon_name=daemon['id']))
                    users = json.loads(RgwUser.list(self, daemon_name=daemon['id']))
                    users_count += len(users)
                    buckets_count += len(buckets)
            else:
                buckets = json.loads(RgwBucket.list(self, daemon_name=daemon['id']))
                users = json.loads(RgwUser.list(self, daemon_name=daemon['id']))
                users_count = len(users)
                buckets_count = len(buckets)

        return {
            'buckets_count': buckets_count,
            'users_count': users_count
        }


@APIRouter('/rgw/user', Scope.RGW)
@APIDoc("RGW User Management API", "RgwUser")
class RgwUser(RgwRESTController):
    @staticmethod
    def _keys_allowed():
        permissions = AuthManager.get_user(JwtManager.get_username()).permissions_dict()
        edit_permissions = [Permission.CREATE, Permission.UPDATE, Permission.DELETE]
        return Scope.RGW in permissions and Permission.READ in permissions[Scope.RGW] \
            and len(set(edit_permissions).intersection(set(permissions[Scope.RGW]))) > 0

    @EndpointDoc("Display RGW Users",
                 responses={200: RGW_USER_SCHEMA})
    def list(self, daemon_name=None):
        # type: (Optional[str]) -> List[str]
        users = []  # type: List[str]
        marker = None
        while True:
            params = {}  # type: dict
            if marker:
                params['marker'] = marker
            result = self.proxy(daemon_name, 'GET', 'user?list', params)
            users.extend(result['keys'])
            if not result['truncated']:
                break
            # Make sure there is a marker.
            assert result['marker']
            # Make sure the marker has changed.
            assert marker != result['marker']
            marker = result['marker']
        return users

    def get(self, uid, daemon_name=None, stats=True) -> dict:
        query_params = '?stats' if stats else ''
        result = self.proxy(daemon_name, 'GET', 'user{}'.format(query_params),
                            {'uid': uid, 'stats': stats})
        if not self._keys_allowed():
            del result['keys']
            del result['swift_keys']
        result['uid'] = result['full_user_id']
        return result

    @Endpoint()
    @ReadPermission
    def get_emails(self, daemon_name=None):
        # type: (Optional[str]) -> List[str]
        emails = []
        for uid in json.loads(self.list(daemon_name)):  # type: ignore
            user = json.loads(self.get(uid, daemon_name))  # type: ignore
            if user["email"]:
                emails.append(user["email"])
        return emails

    @allow_empty_body
    def create(self, uid, display_name, email=None, max_buckets=None,
               system=None, suspended=None, generate_key=None, access_key=None,
               secret_key=None, daemon_name=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if system is not None:
            params['system'] = system
        if suspended is not None:
            params['suspended'] = suspended
        if generate_key is not None:
            params['generate-key'] = generate_key
        if access_key is not None:
            params['access-key'] = access_key
        if secret_key is not None:
            params['secret-key'] = secret_key
        result = self.proxy(daemon_name, 'PUT', 'user', params)
        result['uid'] = result['full_user_id']
        return result

    @allow_empty_body
    def set(self, uid, display_name=None, email=None, max_buckets=None,
            system=None, suspended=None, daemon_name=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if system is not None:
            params['system'] = system
        if suspended is not None:
            params['suspended'] = suspended
        result = self.proxy(daemon_name, 'POST', 'user', params)
        result['uid'] = result['full_user_id']
        return result

    def delete(self, uid, daemon_name=None):
        try:
            instance = RgwClient.admin_instance(daemon_name=daemon_name)
            # Ensure the user is not configured to access the RGW Object Gateway.
            if instance.userid == uid:
                raise DashboardException(msg='Unable to delete "{}" - this user '
                                             'account is required for managing the '
                                             'Object Gateway'.format(uid))
            # Finally redirect request to the RGW proxy.
            return self.proxy(daemon_name, 'DELETE', 'user', {'uid': uid}, json_response=False)
        except (DashboardException, RequestException) as e:  # pragma: no cover
            raise DashboardException(e, component='rgw')

    # pylint: disable=redefined-builtin
    @RESTController.Resource(method='POST', path='/capability', status=201)
    @allow_empty_body
    def create_cap(self, uid, type, perm, daemon_name=None):
        return self.proxy(daemon_name, 'PUT', 'user?caps', {
            'uid': uid,
            'user-caps': '{}={}'.format(type, perm)
        })

    # pylint: disable=redefined-builtin
    @RESTController.Resource(method='DELETE', path='/capability', status=204)
    def delete_cap(self, uid, type, perm, daemon_name=None):
        return self.proxy(daemon_name, 'DELETE', 'user?caps', {
            'uid': uid,
            'user-caps': '{}={}'.format(type, perm)
        })

    @RESTController.Resource(method='POST', path='/key', status=201)
    @allow_empty_body
    def create_key(self, uid, key_type='s3', subuser=None, generate_key='true',
                   access_key=None, secret_key=None, daemon_name=None):
        params = {'uid': uid, 'key-type': key_type, 'generate-key': generate_key}
        if subuser is not None:
            params['subuser'] = subuser
        if access_key is not None:
            params['access-key'] = access_key
        if secret_key is not None:
            params['secret-key'] = secret_key
        return self.proxy(daemon_name, 'PUT', 'user?key', params)

    @RESTController.Resource(method='DELETE', path='/key', status=204)
    def delete_key(self, uid, key_type='s3', subuser=None, access_key=None, daemon_name=None):
        params = {'uid': uid, 'key-type': key_type}
        if subuser is not None:
            params['subuser'] = subuser
        if access_key is not None:
            params['access-key'] = access_key
        return self.proxy(daemon_name, 'DELETE', 'user?key', params, json_response=False)

    @RESTController.Resource(method='GET', path='/quota')
    def get_quota(self, uid, daemon_name=None):
        return self.proxy(daemon_name, 'GET', 'user?quota', {'uid': uid})

    @RESTController.Resource(method='PUT', path='/quota')
    @allow_empty_body
    def set_quota(self, uid, quota_type, enabled, max_size_kb, max_objects, daemon_name=None):
        return self.proxy(daemon_name, 'PUT', 'user?quota', {
            'uid': uid,
            'quota-type': quota_type,
            'enabled': enabled,
            'max-size-kb': max_size_kb,
            'max-objects': max_objects
        }, json_response=False)

    @RESTController.Resource(method='POST', path='/subuser', status=201)
    @allow_empty_body
    def create_subuser(self, uid, subuser, access, key_type='s3',
                       generate_secret='true', access_key=None,
                       secret_key=None, daemon_name=None):
        # pylint: disable=R1705
        subusr_array = []
        user = json.loads(self.get(uid, daemon_name))  # type: ignore
        subusers = user["subusers"]
        for sub_usr in subusers:
            subusr_array.append(sub_usr["id"])
        if subuser in subusr_array:
            return self.proxy(daemon_name, 'POST', 'user', {
                'uid': uid,
                'subuser': subuser,
                'key-type': key_type,
                'access': access,
                'generate-secret': generate_secret,
                'access-key': access_key,
                'secret-key': secret_key
            })
        else:
            return self.proxy(daemon_name, 'PUT', 'user', {
                'uid': uid,
                'subuser': subuser,
                'key-type': key_type,
                'access': access,
                'generate-secret': generate_secret,
                'access-key': access_key,
                'secret-key': secret_key
            })

    @RESTController.Resource(method='DELETE', path='/subuser/{subuser}', status=204)
    def delete_subuser(self, uid, subuser, purge_keys='true', daemon_name=None):
        """
        :param purge_keys: Set to False to do not purge the keys.
                           Note, this only works for s3 subusers.
        """
        return self.proxy(daemon_name, 'DELETE', 'user', {
            'uid': uid,
            'subuser': subuser,
            'purge-keys': purge_keys
        }, json_response=False)


class RGWRoleEndpoints:
    @staticmethod
    def role_list(_):
        rgw_client = RgwClient.admin_instance()
        roles = rgw_client.list_roles()
        return roles

    @staticmethod
    def role_create(_, role_name: str = '', role_path: str = '', role_assume_policy_doc: str = ''):
        assert role_name
        assert role_path
        rgw_client = RgwClient.admin_instance()
        rgw_client.create_role(role_name, role_path, role_assume_policy_doc)
        return f'Role {role_name} created successfully'

    @staticmethod
    def role_update(_, role_name: str, max_session_duration: str):
        assert role_name
        assert max_session_duration
        # convert max_session_duration which is in hours to seconds
        max_session_duration = int(float(max_session_duration) * 3600)
        rgw_client = RgwClient.admin_instance()
        rgw_client.update_role(role_name, str(max_session_duration))
        return f'Role {role_name} updated successfully'

    @staticmethod
    def role_delete(_, role_name: str):
        assert role_name
        rgw_client = RgwClient.admin_instance()
        rgw_client.delete_role(role_name)
        return f'Role {role_name} deleted successfully'

    @staticmethod
    def model(role_name: str):
        assert role_name
        rgw_client = RgwClient.admin_instance()
        role = rgw_client.get_role(role_name)
        model = {'role_name': '', 'max_session_duration': ''}
        model['role_name'] = role['RoleName']

        # convert maxsessionduration which is in seconds to hours
        if role['MaxSessionDuration']:
            model['max_session_duration'] = role['MaxSessionDuration'] / 3600
        return model


# pylint: disable=C0301
assume_role_policy_help = (
    'Paste a json assume role policy document, to find more information on how to get this document, <a '  # noqa: E501
    'href="https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-role.html#cfn-iam-role-assumerolepolicydocument"'  # noqa: E501
    'target="_blank">click here.</a>'
)

max_session_duration_help = (
    'The maximum session duration (in hours) that you want to set for the specified role.This setting can have a value from 1 hour to 12 hours.'  # noqa: E501
)

create_container = VerticalContainer('Create Role', 'create_role', fields=[
    FormField('Role name', 'role_name', validators=[Validator.RGW_ROLE_NAME]),
    FormField('Path', 'role_path', validators=[Validator.RGW_ROLE_PATH]),
    FormField('Assume Role Policy Document',
              'role_assume_policy_doc',
              help=assume_role_policy_help,
              field_type='textarea',
              validators=[Validator.JSON]),
])

edit_container = VerticalContainer('Edit Role', 'edit_role', fields=[
    FormField('Role name', 'role_name', readonly=True),
    FormField('Max Session Duration', 'max_session_duration',
              help=max_session_duration_help,
              validators=[Validator.RGW_ROLE_SESSION_DURATION])
])

create_role_form = Form(path='/create',
                        root_container=create_container,
                        task_info=FormTaskInfo("IAM RGW Role '{role_name}' created successfully",
                                               ['role_name']),
                        method_type=MethodType.POST.value)

edit_role_form = Form(path='/edit',
                      root_container=edit_container,
                      task_info=FormTaskInfo("IAM RGW Role '{role_name}' edited successfully",
                                             ['role_name']),
                      method_type=MethodType.PUT.value,
                      model_callback=RGWRoleEndpoints.model)


@CRUDEndpoint(
    router=APIRouter('/rgw/roles', Scope.RGW),
    doc=APIDoc("List of RGW roles", "RGW"),
    actions=[
        TableAction(name='Create', permission='create', icon=Icon.ADD.value,
                    routerLink='/rgw/roles/create'),
        TableAction(name='Edit', permission='update', icon=Icon.EDIT.value,
                    click='edit', routerLink='/rgw/roles/edit'),
        TableAction(name='Delete', permission='delete', icon=Icon.DESTROY.value,
                    click='delete', disable=True),
    ],
    forms=[create_role_form, edit_role_form],
    column_key='RoleName',
    resource='Role',
    permissions=[Scope.RGW],
    get_all=CRUDCollectionMethod(
        func=RGWRoleEndpoints.role_list,
        doc=EndpointDoc("List RGW roles")
    ),
    create=CRUDCollectionMethod(
        func=RGWRoleEndpoints.role_create,
        doc=EndpointDoc("Create RGW role")
    ),
    edit=CRUDCollectionMethod(
        func=RGWRoleEndpoints.role_update,
        doc=EndpointDoc("Edit RGW role")
    ),
    delete=CRUDCollectionMethod(
        func=RGWRoleEndpoints.role_delete,
        doc=EndpointDoc("Delete RGW role")
    ),
    set_column={
        "CreateDate": {'cellTemplate': 'date'},
        "MaxSessionDuration": {'cellTemplate': 'duration'},
        "RoleId": {'isHidden': True},
        "AssumeRolePolicyDocument": {'isHidden': True},
        "PermissionPolicies": {'isHidden': True},
        "Description": {'isHidden': True},
        "AccountId": {'isHidden': True}
    },
    detail_columns=['RoleId', 'Description',
                    'AssumeRolePolicyDocument', 'PermissionPolicies', 'AccountId'],
    meta=CRUDMeta()
)
class RgwUserRole(NamedTuple):
    RoleId: int
    RoleName: str
    Path: str
    Arn: str
    CreateDate: str
    MaxSessionDuration: int
    AssumeRolePolicyDocument: str
    PermissionPolicies: List
    Description: str
    AccountId: str


@APIRouter('/rgw/realm', Scope.RGW)
class RgwRealm(RESTController):
    @allow_empty_body
    # pylint: disable=W0613
    def create(self, realm_name, default):
        multisite_instance = RgwMultisite()
        result = multisite_instance.create_realm(realm_name, default)
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def list(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.list_realms()
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def get(self, realm_name):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_realm(realm_name)
        return result

    @Endpoint()
    @ReadPermission
    def get_all_realms_info(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_all_realms_info()
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def set(self, realm_name: str, new_realm_name: str, default: str = ''):
        multisite_instance = RgwMultisite()
        result = multisite_instance.edit_realm(realm_name, new_realm_name, default)
        return result

    @Endpoint()
    @ReadPermission
    def get_realm_tokens(self):
        try:
            result = CephService.get_realm_tokens()
            return result
        except NoRgwDaemonsException as e:
            raise DashboardException(e, http_status_code=404, component='rgw')

    @Endpoint(method='POST')
    @UpdatePermission
    @allow_empty_body
    # pylint: disable=W0613
    def import_realm_token(self, realm_token, zone_name, port, placement_spec):
        try:
            multisite_instance = RgwMultisite()
            result = CephService.import_realm_token(realm_token, zone_name, port, placement_spec)
            multisite_instance.update_period()
            return result
        except NoRgwDaemonsException as e:
            raise DashboardException(e, http_status_code=404, component='rgw')

    def delete(self, realm_name):
        multisite_instance = RgwMultisite()
        result = multisite_instance.delete_realm(realm_name)
        return result


@APIRouter('/rgw/zonegroup', Scope.RGW)
class RgwZonegroup(RESTController):
    @allow_empty_body
    # pylint: disable=W0613
    def create(self, realm_name, zonegroup_name, default=None, master=None,
               zonegroup_endpoints=None):
        multisite_instance = RgwMultisite()
        result = multisite_instance.create_zonegroup(realm_name, zonegroup_name, default,
                                                     master, zonegroup_endpoints)
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def list(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.list_zonegroups()
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def get(self, zonegroup_name):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_zonegroup(zonegroup_name)
        return result

    @Endpoint()
    @ReadPermission
    def get_all_zonegroups_info(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_all_zonegroups_info()
        return result

    def delete(self, zonegroup_name, delete_pools, pools: Optional[List[str]] = None):
        if pools is None:
            pools = []
        try:
            multisite_instance = RgwMultisite()
            result = multisite_instance.delete_zonegroup(zonegroup_name, delete_pools, pools)
            return result
        except NoRgwDaemonsException as e:
            raise DashboardException(e, http_status_code=404, component='rgw')

    @allow_empty_body
    # pylint: disable=W0613,W0102
    def set(self, zonegroup_name: str, realm_name: str, new_zonegroup_name: str,
            default: str = '', master: str = '', zonegroup_endpoints: str = '',
            add_zones: List[str] = [], remove_zones: List[str] = [],
            placement_targets: List[Dict[str, str]] = []):
        multisite_instance = RgwMultisite()
        result = multisite_instance.edit_zonegroup(realm_name, zonegroup_name, new_zonegroup_name,
                                                   default, master, zonegroup_endpoints, add_zones,
                                                   remove_zones, placement_targets)
        return result


@APIRouter('/rgw/zone', Scope.RGW)
class RgwZone(RESTController):
    @allow_empty_body
    # pylint: disable=W0613
    def create(self, zone_name, zonegroup_name=None, default=False, master=False,
               zone_endpoints=None, access_key=None, secret_key=None):
        multisite_instance = RgwMultisite()
        result = multisite_instance.create_zone(zone_name, zonegroup_name, default,
                                                master, zone_endpoints, access_key,
                                                secret_key)
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def list(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.list_zones()
        return result

    @allow_empty_body
    # pylint: disable=W0613
    def get(self, zone_name):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_zone(zone_name)
        return result

    @Endpoint()
    @ReadPermission
    def get_all_zones_info(self):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_all_zones_info()
        return result

    def delete(self, zone_name, delete_pools, pools: Optional[List[str]] = None,
               zonegroup_name=None):
        if pools is None:
            pools = []
        if zonegroup_name is None:
            zonegroup_name = ''
        try:
            multisite_instance = RgwMultisite()
            result = multisite_instance.delete_zone(zone_name, delete_pools, pools, zonegroup_name)
            return result
        except NoRgwDaemonsException as e:
            raise DashboardException(e, http_status_code=404, component='rgw')

    @allow_empty_body
    # pylint: disable=W0613,W0102
    def set(self, zone_name: str, new_zone_name: str, zonegroup_name: str, default: str = '',
            master: str = '', zone_endpoints: str = '', access_key: str = '', secret_key: str = '',
            placement_target: str = '', data_pool: str = '', index_pool: str = '',
            data_extra_pool: str = '', storage_class: str = '', data_pool_class: str = '',
            compression: str = ''):
        multisite_instance = RgwMultisite()
        result = multisite_instance.edit_zone(zone_name, new_zone_name, zonegroup_name, default,
                                              master, zone_endpoints, access_key, secret_key,
                                              placement_target, data_pool, index_pool,
                                              data_extra_pool, storage_class, data_pool_class,
                                              compression)
        return result

    @Endpoint()
    @ReadPermission
    def get_pool_names(self):
        pool_names = []
        ret, out, _ = mgr.check_mon_command({
            'prefix': 'osd lspools',
            'format': 'json',
        })
        if ret == 0 and out is not None:
            pool_names = json.loads(out)
        return pool_names

    @Endpoint('PUT')
    @CreatePermission
    def create_system_user(self, userName: str, zoneName: str):
        multisite_instance = RgwMultisite()
        result = multisite_instance.create_system_user(userName, zoneName)
        return result

    @Endpoint()
    @ReadPermission
    def get_user_list(self, zoneName=None):
        multisite_instance = RgwMultisite()
        result = multisite_instance.get_user_list(zoneName)
        return result
