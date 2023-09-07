# -*- coding: utf-8 -*-

import json
import logging
import re
from typing import Any, Dict, List, NamedTuple, Optional, Union

import cherrypy

from ..exceptions import DashboardException
from ..rest_client import RequestException
from ..security import Permission, Scope
from ..services.auth import AuthManager, JwtManager
from ..services.ceph_service import CephService
from ..services.rgw_client import NoRgwDaemonsException, RgwClient
from ..tools import json_str_to_object, str_to_bool
from . import APIDoc, APIRouter, BaseController, CRUDCollectionMethod, \
    CRUDEndpoint, Endpoint, EndpointDoc, ReadPermission, RESTController, \
    UIRouter, allow_empty_body
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

                # extract per-daemon service data and health
                daemon = {
                    'id': metadata['id'],
                    'service_map_id': service['id'],
                    'version': metadata['ceph_version'],
                    'server_hostname': hostname,
                    'realm_name': metadata['realm_name'],
                    'zonegroup_name': metadata['zonegroup_name'],
                    'zone_name': metadata['zone_name'],
                    'default': instance.daemon.name == metadata['id'],
                    'port': int(re.findall(r'port=(\d+)', metadata['frontend_config#0'])[0])
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

        # Append the locking configuration.
        locking = self._get_locking(result['owner'], daemon_name, bucket_name)
        result.update(locking)

        return self._append_bid(result)

    @allow_empty_body
    def create(self, bucket, uid, zonegroup=None, placement_target=None,
               lock_enabled='false', lock_mode=None,
               lock_retention_period_days=None,
               lock_retention_period_years=None, encryption_state='false',
               encryption_type=None, key_id=None, daemon_name=None):
        lock_enabled = str_to_bool(lock_enabled)
        encryption_state = str_to_bool(encryption_state)
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

            return result
        except RequestException as e:  # pragma: no cover - handling is too obvious
            raise DashboardException(e, http_status_code=500, component='rgw')

    @allow_empty_body
    def set(self, bucket, bucket_id, uid, versioning_state=None,
            encryption_state='false', encryption_type=None, key_id=None,
            mfa_delete=None, mfa_token_serial=None, mfa_token_pin=None,
            lock_mode=None, lock_retention_period_days=None,
            lock_retention_period_years=None, daemon_name=None):
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


@APIRouter('/rgw/user', Scope.RGW)
@APIDoc("RGW User Management API", "RgwUser")
class RgwUser(RgwRESTController):
    def _append_uid(self, user):
        """
        Append the user identifier that looks like [<tenant>$]<user>.
        See http://docs.ceph.com/docs/jewel/radosgw/multitenancy/ for
        more information.
        :param user: The user parameters.
        :type user: dict
        :return: The modified user parameters including the 'uid' parameter.
        :rtype: dict
        """
        if isinstance(user, dict):
            user['uid'] = '{}${}'.format(user['tenant'], user['user_id']) \
                if user['tenant'] else user['user_id']
        return user

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
        return self._append_uid(result)

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
               suspended=None, generate_key=None, access_key=None,
               secret_key=None, daemon_name=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if suspended is not None:
            params['suspended'] = suspended
        if generate_key is not None:
            params['generate-key'] = generate_key
        if access_key is not None:
            params['access-key'] = access_key
        if secret_key is not None:
            params['secret-key'] = secret_key
        result = self.proxy(daemon_name, 'PUT', 'user', params)
        return self._append_uid(result)

    @allow_empty_body
    def set(self, uid, display_name=None, email=None, max_buckets=None,
            suspended=None, daemon_name=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if suspended is not None:
            params['suspended'] = suspended
        result = self.proxy(daemon_name, 'POST', 'user', params)
        return self._append_uid(result)

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


# pylint: disable=C0301
assume_role_policy_help = (
    'Paste a json assume role policy document, to find more information on how to get this document, <a '  # noqa: E501
    'href="https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-role.html#cfn-iam-role-assumerolepolicydocument"'  # noqa: E501
    'target="_blank">click here.</a>'
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
create_role_form = Form(path='/rgw/roles/create',
                        root_container=create_container,
                        task_info=FormTaskInfo("IAM RGW Role '{role_name}' created successfully",
                                               ['role_name']),
                        method_type=MethodType.POST.value)


@CRUDEndpoint(
    router=APIRouter('/rgw/roles', Scope.RGW),
    doc=APIDoc("List of RGW roles", "RGW"),
    actions=[
        TableAction(name='Create', permission='create', icon=Icon.ADD.value,
                    routerLink='/rgw/roles/create')
    ],
    forms=[create_role_form],
    permissions=[Scope.CONFIG_OPT],
    get_all=CRUDCollectionMethod(
        func=RGWRoleEndpoints.role_list,
        doc=EndpointDoc("List RGW roles")
    ),
    create=CRUDCollectionMethod(
        func=RGWRoleEndpoints.role_create,
        doc=EndpointDoc("Create Ceph User")
    ),
    set_column={
        "CreateDate": {'cellTemplate': 'date'},
        "MaxSessionDuration": {'cellTemplate': 'duration'},
        "RoleId": {'isHidden': True},
        "AssumeRolePolicyDocument": {'isHidden': True}
    },
    detail_columns=['RoleId', 'AssumeRolePolicyDocument'],
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
