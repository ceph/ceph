# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from ..tools import ApiController, RESTController, AuthRequired
from .. import logger


@ApiController('rgw')
@AuthRequired()
class Rgw(RESTController):

    def __init__(self):
        self.daemon = RgwDaemon(self.mgr)


class RgwDaemon(RESTController):

    def __init__(self, mgr):
        RgwDaemon.mgr = mgr

    def list(self):
        daemons = []
        for server in self.mgr.list_servers():
            for service in server['services']:
                if service['type'] == 'rgw':
                    metadata = self.mgr.get_metadata('rgw', service['id'])
                    status = self.mgr.get_daemon_status('rgw', service['id'])
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
                        'server_hostname': server['hostname']
                    }

                    daemons.append(daemon)

        return sorted(daemons, key=lambda k: k['id'])

    def get(self, svc_id):
        daemon = {
            'rgw_metadata': [],
            'rgw_id': svc_id,
            'rgw_status': []
        }
        for server in self.mgr.list_servers():
            for service in server['services']:
                if service['type'] == 'rgw' and service['id'] == svc_id:
                    metadata = self.mgr.get_metadata('rgw', service['id'])
                    status = self.mgr.get_daemon_status('rgw', service['id'])
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

                    break
        return daemon
