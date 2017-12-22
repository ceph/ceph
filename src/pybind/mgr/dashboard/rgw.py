# -*- code: utf-8 -*-

import json
from remote_view_cache import RemoteViewCache

import logging

log = logging.getLogger("dashboard")


class RGWDaemons(RemoteViewCache):

    def _get(self):
        daemons = self.get_daemons()
        return {
            'daemons': daemons,
        }

    def get_daemons(self):
        daemons = []
        for server in self._module.list_servers():
            for service in server['services']:
                if service['type'] == 'rgw':
                    metadata = self._module.get_metadata('rgw', service['id'])
                    status = self._module.get_daemon_status('rgw', service['id'])
                    try:
                        status = json.loads(status['json'])
                    except:
                        log.warn("{0} had invalid status json".format(service['id']))
                        status = {}
                    
                    # extract per-daemon service data and health
                    daemon = {
                        'id': service['id'],
                        'version': metadata['ceph_version'],
                        'server_hostname': server['hostname'],
                        'service': service,
                        'server': server,
                        'metadata': metadata,
                        'status': status,
                        'url': "{0}/rgw/{1}".format(self._module.url_prefix, service['id'])
                    }

                    daemons.append(daemon)
        return sorted(daemons, key=lambda k: k['id'])
