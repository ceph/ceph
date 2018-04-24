# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController
from .. import mgr
from ..services.ceph_service import CephService


@ApiController('/cluster_conf')
class ClusterConfiguration(RESTController):

    def _append_config_option_values(self, options):
        """
        Appends values from the config database (if available) to the given options
        :param options: list of config options
        :return: list of config options extended by their current values
        """
        config_dump = CephService.send_command('mon', 'config dump')
        for config_dump_entry in config_dump:
            for i, elem in enumerate(options):
                if config_dump_entry['name'] == elem['name']:
                    if 'value' not in elem:
                        options[i]['value'] = []
                        options[i]['source'] = 'mon'

                    options[i]['value'].append({'section': config_dump_entry['section'],
                                                'value': config_dump_entry['value']})
        return options

    def list(self):
        options = mgr.get("config_options")['options']
        return self._append_config_option_values(options)

    def get(self, name):
        for option in mgr.get('config_options')['options']:
            if option['name'] == name:
                return self._append_config_option_values([option])[0]

        raise cherrypy.HTTPError(404)
