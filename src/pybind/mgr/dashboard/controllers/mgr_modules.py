# -*- coding: utf-8 -*-

from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.exception import handle_send_command_error
from ..tools import find_object_in_list, str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, RESTController, allow_empty_body

MGR_MODULE_SCHEMA = ([{
    "name": (str, "Module Name"),
    "enabled": (bool, "Is Module Enabled"),
    "always_on": (bool, "Is it an always on module?"),
    "options": ({
        "Option_name": ({
            "name": (str, "Name of the option"),
            "type": (str, "Type of the option"),
            "level": (str, "Option level"),
            "flags": (int, "List of flags associated"),
            "default_value": (int, "Default value for the option"),
            "min": (str, "Minimum value"),
            "max": (str, "Maximum value"),
            "enum_allowed": ([str], ""),
            "desc": (str, "Description of the option"),
            "long_desc": (str, "Elaborated description"),
            "tags": ([str], "Tags associated with the option"),
            "see_also": ([str], "Related options")
        }, "Options")
    }, "Module Options")
}])


@APIRouter('/mgr/module', Scope.CONFIG_OPT)
@APIDoc("Get details of MGR Module", "MgrModule")
class MgrModules(RESTController):
    ignore_modules = ['selftest']

    @EndpointDoc("List Mgr modules",
                 responses={200: MGR_MODULE_SCHEMA})
    def list(self):
        """
        Get the list of managed modules.
        :return: A list of objects with the fields 'enabled', 'name' and 'options'.
        :rtype: list
        """
        result = []
        mgr_map = mgr.get('mgr_map')
        always_on_modules = mgr_map['always_on_modules'].get(mgr.release_name, [])
        for module_config in mgr_map['available_modules']:
            module_name = module_config['name']
            if module_name not in self.ignore_modules:
                always_on = module_name in always_on_modules
                enabled = module_name in mgr_map['modules'] or always_on
                result.append({
                    'name': module_name,
                    'enabled': enabled,
                    'always_on': always_on,
                    'options': self._convert_module_options(
                        module_config['module_options'])
                })
        return result

    def get(self, module_name):
        """
        Retrieve the values of the persistent configuration settings.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        :return: The values of the module options.
        :rtype: dict
        """
        assert self._is_module_managed(module_name)
        options = self._get_module_options(module_name)
        result = {}
        for name, option in options.items():
            result[name] = mgr.get_module_option_ex(module_name, name,
                                                    option['default_value'])
        return result

    @RESTController.Resource('PUT')
    def set(self, module_name, config):
        """
        Set the values of the persistent configuration settings.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        :param config: The values of the module options to be stored.
        :type config: dict
        """
        assert self._is_module_managed(module_name)
        options = self._get_module_options(module_name)
        for name in options.keys():
            if name in config:
                mgr.set_module_option_ex(module_name, name, config[name])

    @RESTController.Resource('POST')
    @handle_send_command_error('mgr_modules')
    @allow_empty_body
    def enable(self, module_name):
        """
        Enable the specified Ceph Mgr module.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        """
        assert self._is_module_managed(module_name)
        CephService.send_command(
            'mon', 'mgr module enable', module=module_name)

    @RESTController.Resource('POST')
    @handle_send_command_error('mgr_modules')
    @allow_empty_body
    def disable(self, module_name):
        """
        Disable the specified Ceph Mgr module.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        """
        assert self._is_module_managed(module_name)
        CephService.send_command(
            'mon', 'mgr module disable', module=module_name)

    @RESTController.Resource('GET')
    def options(self, module_name):
        """
        Get the module options of the specified Ceph Mgr module.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        :return: The module options as list of dicts.
        :rtype: list
        """
        assert self._is_module_managed(module_name)
        return self._get_module_options(module_name)

    def _is_module_managed(self, module_name):
        """
        Check if the specified Ceph Mgr module is managed by this service.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        :return: Returns ``true`` if the Ceph Mgr module is managed by
            this service, otherwise ``false``.
        :rtype: bool
        """
        if module_name in self.ignore_modules:
            return False
        mgr_map = mgr.get('mgr_map')
        for module_config in mgr_map['available_modules']:
            if module_name == module_config['name']:
                return True
        return False

    def _get_module_config(self, module_name):
        """
        Helper function to get detailed module configuration.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        :return: The module information, e.g. module name, can run,
            error string and available module options.
        :rtype: dict or None
        """
        mgr_map = mgr.get('mgr_map')
        return find_object_in_list('name', module_name,
                                   mgr_map['available_modules'])

    def _get_module_options(self, module_name):
        """
        Helper function to get the module options.
        :param module_name: The name of the Ceph Mgr module.
        :type module_name: str
        :return: The module options.
        :rtype: dict
        """
        options = self._get_module_config(module_name)['module_options']
        return self._convert_module_options(options)

    def _convert_module_options(self, options):
        # Workaround a possible bug in the Ceph Mgr implementation.
        # Various fields (e.g. default_value, min, max) are always
        # returned as a string.
        for option in options.values():
            if option['type'] == 'str':
                if option['default_value'] == 'None':  # This is Python None
                    option['default_value'] = ''
            elif option['type'] == 'bool':
                if option['default_value'] == '':
                    option['default_value'] = False
                else:
                    option['default_value'] = str_to_bool(
                        option['default_value'])
            elif option['type'] in ['float', 'uint', 'int', 'size', 'secs']:
                cls = {
                    'float': float
                }.get(option['type'], int)
                for name in ['default_value', 'min', 'max']:
                    if option[name] == 'None':  # This is Python None
                        option[name] = None
                    elif option[name]:  # Skip empty entries
                        option[name] = cls(option[name])
        return options
