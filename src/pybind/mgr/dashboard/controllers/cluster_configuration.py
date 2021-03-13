# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService
from . import ApiController, ControllerDoc, EndpointDoc, RESTController

FILTER_SCHEMA = [{
    "name": (str, 'Name of the config option'),
    "type": (str, 'Config option type'),
    "level": (str, 'Config option level'),
    "desc": (str, 'Description of the configuration'),
    "long_desc": (str, 'Elaborated description'),
    "default": (str, 'Default value for the config option'),
    "daemon_default": (str, 'Daemon specific default value'),
    "tags": ([str], 'Tags associated with the cluster'),
    "services": ([str], 'Services associated with the config option'),
    "see_also": ([str], 'Related config options'),
    "enum_values": ([str], 'List of enums allowed'),
    "min": (str, 'Minimum value'),
    "max": (str, 'Maximum value'),
    "can_update_at_runtime": (bool, 'Check if can update at runtime'),
    "flags": ([str], 'List of flags associated')
}]


@ApiController('/cluster_conf', Scope.CONFIG_OPT)
@ControllerDoc("Manage Cluster Configurations", "ClusterConfiguration")
class ClusterConfiguration(RESTController):

    def _append_config_option_values(self, options):
        """
        Appends values from the config database (if available) to the given options
        :param options: list of config options
        :return: list of config options extended by their current values
        """
        config_dump = CephService.send_command('mon', 'config dump')
        mgr_config = mgr.get('config')
        config_dump.append({'name': 'fsid', 'section': 'mgr', 'value': mgr_config['fsid']})

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

    @RESTController.Collection('GET', query_params=['name'])
    @EndpointDoc("Get Cluster Configuration by name",
                 parameters={
                     'names': (str, 'Config option names'),
                 },
                 responses={200: FILTER_SCHEMA})
    def filter(self, names=None):
        config_options = []

        if names:
            for name in names.split(','):
                try:
                    config_options.append(self._get_config_option(name))
                except cherrypy.HTTPError:
                    pass

        if not config_options:
            raise cherrypy.HTTPError(404, 'Config options `{}` not found'.format(names))

        return config_options

    def create(self, name, value):
        # Check if config option is updateable at runtime
        self._updateable_at_runtime([name])

        # Update config option
        avail_sections = ['global', 'mon', 'mgr', 'osd', 'mds', 'client']

        for section in avail_sections:
            for entry in value:
                if entry['value'] is None:
                    break

                if entry['section'] == section:
                    CephService.send_command('mon', 'config set', who=section, name=name,
                                             value=str(entry['value']))
                    break
            else:
                CephService.send_command('mon', 'config rm', who=section, name=name)

    def delete(self, name, section):
        return CephService.send_command('mon', 'config rm', who=section, name=name)

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
