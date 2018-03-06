# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from .. import logger
from ..services.ceph_service import CephService
from ..tools import ApiController, RESTController, AuthRequired


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
                status = service['status']
                if 'json' in status:
                    try:
                        status = json.loads(status['json'])
                    except ValueError:
                        logger.warning("%s had invalid status json", service['id'])
                        status = {}
                else:
                    logger.warning('%s has no key "json" in status', service['id'])

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
