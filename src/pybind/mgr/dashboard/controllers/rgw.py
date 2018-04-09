# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import cherrypy

from . import ApiController, BaseController, RESTController, AuthRequired
from .. import logger
from ..services.ceph_service import CephService
from ..services.rgw_client import RgwClient
from ..rest_client import RequestException
from ..exceptions import NoCredentialsException


@ApiController('rgw')
@AuthRequired()
class Rgw(RESTController):
    pass


@ApiController('rgw/daemon')
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
            return daemon

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


@ApiController('rgw/proxy/{path:.*}')
@AuthRequired()
class RgwProxy(BaseController):
    @cherrypy.expose
    def __call__(self, path, **params):
        try:
            rgw_client = RgwClient.admin_instance()

        except NoCredentialsException as e:
            cherrypy.response.headers['Content-Type'] = 'application/json'
            cherrypy.response.status = 401
            return json.dumps({'message': str(e)}).encode('utf-8')

        method = cherrypy.request.method
        data = None

        if cherrypy.request.body.length:
            data = cherrypy.request.body.read()

        try:
            return rgw_client.proxy(method, path, params, data)
        except RequestException as e:
            cherrypy.response.status = e.status_code
            return e.content
