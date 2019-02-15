# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController
from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from ..exceptions import DashboardException


@ApiController('/cluster_conf', Scope.CONFIG_OPT)
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
        options = mgr.get('config_options')['options']
        return self._append_config_option_values(options)

    def get(self, name):
        return self._get_config_option(name)

    def create(self, name, value):
        # Check if config option is updateable at runtime
        self._updateable_at_runtime([name])

        # Update config option
        availSections = ['global', 'mon', 'mgr', 'osd', 'mds', 'client']

        for section in availSections:
            for entry in value:
                if not entry['value']:
                    break

                if entry['section'] == section:
                    CephService.send_command('mon', 'config set', who=section, name=name,
                                             value=str(entry['value']))
                    break
            else:
                CephService.send_command('mon', 'config rm', who=section, name=name)

    def bulk_set(self, options):
        self._updateable_at_runtime(options.keys())

        for name, value in options.items():
            CephService.send_command('mon', 'config set', who=value['section'],
                                     name=name, value=str(value['value']))

    def _get_config_option(self, name):
        for option in mgr.get('config_options')['options']:
            if option['name'] == name:
                return self._append_config_option_values([option])[0]

        raise cherrypy.HTTPError(404)

    def _updateable_at_runtime(self, config_option_names):
        not_updateable = []

        for name in config_option_names:
            config_option = self._get_config_option(name)
            if not config_option['can_update_at_runtime']:
                not_updateable.append(name)

        if not_updateable:
            raise DashboardException(
                msg='Config option {} is/are not updatable at runtime'.format(
                    ', '.join(not_updateable)),
                code='config_option_not_updatable_at_runtime',
                component='cluster_configuration')
