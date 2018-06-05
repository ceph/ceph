# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import cherrypy

from . import ApiController, BaseController, RESTController, AuthRequired, \
              Endpoint, Proxy
from .. import logger
from ..services.ceph_service import CephService
from ..services.rgw_client import RgwClient
from ..rest_client import RequestException


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
                logger.warning("%s had invalid status json", service['id'])
                status = {}
        else:
            logger.warning('%s has no key "json" in status', service['id'])

        daemon['rgw_metadata'] = metadata
        daemon['rgw_status'] = status
        return daemon


@ApiController('/rgw/proxy')
@AuthRequired()
class RgwProxy(BaseController):

    @Proxy()
    def __call__(self, path, **params):
        try:
            rgw_client = RgwClient.admin_instance()

            method = cherrypy.request.method
            data = None

            if cherrypy.request.body.length:
                data = cherrypy.request.body.read()

            return rgw_client.proxy(method, path, params, data)
        except RequestException as e:
            # Always use status code 500 and NOT the status that may delivered
            # by the exception. That's because we do not want to forward e.g.
            # 401 or 404 that may trigger unwanted actions in the UI.
            cherrypy.response.headers['Content-Type'] = 'application/json'
            cherrypy.response.status = 500
            return json.dumps({'detail': str(e)}).encode('utf-8')


@ApiController('/rgw/bucket')
@AuthRequired()
class RgwBucket(RESTController):

    def create(self, bucket, uid):
        try:
            rgw_client = RgwClient.instance(uid)
            return rgw_client.create_bucket(bucket)
        except RequestException as e:
            cherrypy.response.headers['Content-Type'] = 'application/json'
            cherrypy.response.status = 500
            return {'detail': str(e)}
