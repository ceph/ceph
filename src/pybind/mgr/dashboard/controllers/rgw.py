# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import cherrypy

from . import ApiController, BaseController, RESTController, AuthRequired, Endpoint
from .. import logger
from ..services.ceph_service import CephService
from ..services.rgw_client import RgwClient
from ..rest_client import RequestException
from ..exceptions import DashboardException


@ApiController('/rgw')
@AuthRequired()
class Rgw(BaseController):

    @Endpoint()
    def status(self):
        status = {'available': False, 'message': None}
        try:
            instance = RgwClient.admin_instance()
            # Check if the service is online.
            if not instance.is_service_online():
                status['message'] = 'Failed to connect to the Object Gateway\'s Admin Ops API.'
                raise RequestException(status['message'])
            # Ensure the API user ID is known by the RGW.
            if not instance.is_system_user():
                status['message'] = 'The user "{}" is unknown to the Object Gateway.'.format(
                    instance.userid)
                raise RequestException(status['message'])
            status['available'] = True
        except RequestException:
            pass
        return status


@ApiController('/rgw/daemon')
@AuthRequired()
class RgwDaemon(RESTController):

    def list(self):
        daemons = []
        for hostname, server in CephService.get_service_map('rgw').items():
            for service in server['services']:
                metadata = service['metadata']

                # extract per-daemon service data and health
                daemon = {
                    'id': service['id'],
                    'version': metadata['ceph_version'],
                    'server_hostname': hostname
                }

                daemons.append(daemon)

        return sorted(daemons, key=lambda k: k['id'])

    def get(self, svc_id):
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

    def proxy(self, method, path, params=None, json_response=True):
        try:
            instance = RgwClient.admin_instance()
            result = instance.proxy(method, path, params, None)
            if json_response and result != '':
                result = json.loads(result.decode('utf-8'))
            return result
        except (DashboardException, RequestException) as e:
            raise DashboardException(e, http_status_code=500, component='rgw')


@ApiController('/rgw/bucket')
@AuthRequired()
class RgwBucket(RgwRESTController):

    def list(self):
        return self.proxy('GET', 'bucket')

    def get(self, bucket):
        return self.proxy('GET', 'bucket', {'bucket': bucket})

    def create(self, bucket, uid):
        try:
            rgw_client = RgwClient.instance(uid)
            return rgw_client.create_bucket(bucket)
        except RequestException as e:
            raise DashboardException(e, http_status_code=500, component='rgw')

    def set(self, bucket, bucket_id, uid):
        return self.proxy('PUT', 'bucket', {
            'bucket': bucket,
            'bucket-id': bucket_id,
            'uid': uid
        }, json_response=False)

    def delete(self, bucket, purge_objects='true'):
        return self.proxy('DELETE', 'bucket', {
            'bucket': bucket,
            'purge-objects': purge_objects
        }, json_response=False)


@ApiController('/rgw/user')
@AuthRequired()
class RgwUser(RgwRESTController):

    def list(self):
        return self.proxy('GET', 'metadata/user')

    def get(self, uid):
        return self.proxy('GET', 'user', {'uid': uid})

    def create(self, uid, display_name, email=None, max_buckets=None,
               suspended=None, generate_key=None, access_key=None,
               secret_key=None):
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
        return self.proxy('PUT', 'user', params)

    def set(self, uid, display_name=None, email=None, max_buckets=None,
            suspended=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if suspended is not None:
            params['suspended'] = suspended
        return self.proxy('POST', 'user', params)

    def delete(self, uid):
        try:
            instance = RgwClient.admin_instance()
            # Ensure the user is not configured to access the RGW Object Gateway.
            if instance.userid == uid:
                raise DashboardException(msg='Unable to delete "{}" - this user '
                                             'account is required for managing the '
                                             'Object Gateway'.format(uid))
            # Finally redirect request to the RGW proxy.
            return self.proxy('DELETE', 'user', {'uid': uid}, json_response=False)
        except (DashboardException, RequestException) as e:
            raise DashboardException(e, component='rgw')

    # pylint: disable=redefined-builtin
    @RESTController.Resource(method='POST', path='/capability', status=201)
    def create_cap(self, uid, type, perm):
        return self.proxy('PUT', 'user?caps', {
            'uid': uid,
            'user-caps': '{}={}'.format(type, perm)
        })

    # pylint: disable=redefined-builtin
    @RESTController.Resource(method='DELETE', path='/capability', status=204)
    def delete_cap(self, uid, type, perm):
        return self.proxy('DELETE', 'user?caps', {
            'uid': uid,
            'user-caps': '{}={}'.format(type, perm)
        })

    @RESTController.Resource(method='POST', path='/key', status=201)
    def create_key(self, uid, key_type='s3', subuser=None, generate_key='true',
                   access_key=None, secret_key=None):
        params = {'uid': uid, 'key-type': key_type, 'generate-key': generate_key}
        if subuser is not None:
            params['subuser'] = subuser
        if access_key is not None:
            params['access-key'] = access_key
        if secret_key is not None:
            params['secret-key'] = secret_key
        return self.proxy('PUT', 'user?key', params)

    @RESTController.Resource(method='DELETE', path='/key', status=204)
    def delete_key(self, uid, key_type='s3', subuser=None, access_key=None):
        params = {'uid': uid, 'key-type': key_type}
        if subuser is not None:
            params['subuser'] = subuser
        if access_key is not None:
            params['access-key'] = access_key
        return self.proxy('DELETE', 'user?key', params, json_response=False)

    @RESTController.Resource(method='GET', path='/quota')
    def get_quota(self, uid):
        return self.proxy('GET', 'user?quota', {'uid': uid})

    @RESTController.Resource(method='PUT', path='/quota')
    def set_quota(self, uid, quota_type, enabled, max_size_kb, max_objects):
        return self.proxy('PUT', 'user?quota', {
            'uid': uid,
            'quota-type': quota_type,
            'enabled': enabled,
            'max-size-kb': max_size_kb,
            'max-objects': max_objects
        }, json_response=False)

    @RESTController.Resource(method='POST', path='/subuser', status=201)
    def create_subuser(self, uid, subuser, access, key_type='s3',
                       generate_secret='true', access_key=None,
                       secret_key=None):
        return self.proxy('PUT', 'user', {
            'uid': uid,
            'subuser': subuser,
            'key-type': key_type,
            'access': access,
            'generate-secret': generate_secret,
            'access-key': access_key,
            'secret-key': secret_key
        })

    @RESTController.Resource(method='DELETE', path='/subuser/{subuser}', status=204)
    def delete_subuser(self, uid, subuser, purge_keys='true'):
        """
        :param purge_keys: Set to False to do not purge the keys.
                           Note, this only works for s3 subusers.
        """
        return self.proxy('DELETE', 'user', {
            'uid': uid,
            'subuser': subuser,
            'purge-keys': purge_keys
        }, json_response=False)
