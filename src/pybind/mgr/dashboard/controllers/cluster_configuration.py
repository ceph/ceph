# -*- coding: utf-8 -*-

from typing import Optional

import cherrypy

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService
from . import APIDoc, APIRouter, EndpointDoc, Param, RESTController

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


@APIRouter('/cluster_conf', Scope.CONFIG_OPT)
@APIDoc("Manage Cluster Configurations", "ClusterConfiguration")
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

    @EndpointDoc("Create/Update Cluster Configuration",
                 parameters={
                     'name': Param(str, 'Config option name'),
                     'value': (
                         [
                            {
                                'section': Param(
                                    str, 'Section/Client where config needs to be updated'
                                ),
                                'value': Param(str, 'Value of the config option')
                            }
                         ], 'Section and Value of the config option'
                     ),
                     'force_update': Param(bool, 'Force update the config option', False, None)
                 }
                 )
    def create(self, name, value, force_update: Optional[bool] = None):
        # Check if config option is updateable at runtime
        self._updateable_at_runtime([name], force_update)

        for entry in value:
            section = entry['section']
            entry_value = entry['value']

            if entry_value not in (None, ''):
                CephService.send_command('mon', 'config set', who=section, name=name,
                                         value=str(entry_value))
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

    def _updateable_at_runtime(self, config_option_names, force_update=False):
        not_updateable = []

        for name in config_option_names:
            config_option = self._get_config_option(name)

            # making rgw configuration to be editable by bypassing 'can_update_at_runtime'
            # as the same can be done via CLI.
            if force_update and 'rgw' in name and not config_option['can_update_at_runtime']:
                break

            if force_update and 'rgw' not in name and not config_option['can_update_at_runtime']:
                raise DashboardException(
                    msg=f'Only the configuration containing "rgw" can be edited at runtime with'
                        f' force_update flag, hence not able to update "{name}"',
                    code='config_option_not_updatable_at_runtime',
                    component='cluster_configuration'
                )
            if not config_option['can_update_at_runtime']:
                not_updateable.append(name)

        if not_updateable:
            raise DashboardException(
                msg='Config option {} is/are not updatable at runtime'.format(
                    ', '.join(not_updateable)),
                code='config_option_not_updatable_at_runtime',
                component='cluster_configuration')
