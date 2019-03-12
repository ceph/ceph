# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno
import inspect
from six import add_metaclass

from . import mgr


class Options(object):
    """
    If you need to store some configuration value please add the config option
    name as a class attribute to this class.

    Example::

        GRAFANA_API_HOST = ('localhost', str)
        GRAFANA_API_PORT = (3000, int)
    """
    ENABLE_BROWSABLE_API = (True, bool)
    REST_REQUESTS_TIMEOUT = (45, int)

    # API auditing
    AUDIT_API_ENABLED = (False, bool)
    AUDIT_API_LOG_PAYLOAD = (True, bool)

    # RGW settings
    RGW_API_HOST = ('', str)
    RGW_API_PORT = (80, int)
    RGW_API_ACCESS_KEY = ('', str)
    RGW_API_SECRET_KEY = ('', str)
    RGW_API_ADMIN_RESOURCE = ('admin', str)
    RGW_API_SCHEME = ('http', str)
    RGW_API_USER_ID = ('', str)
    RGW_API_SSL_VERIFY = (True, bool)

    # Grafana settings
    GRAFANA_API_URL = ('', str)
    GRAFANA_API_USERNAME = ('admin', str)
    GRAFANA_API_PASSWORD = ('admin', str)

    # NFS Ganesha settings
    GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = ('', str)

    # Prometheus settings
    PROMETHEUS_API_HOST = ('', str)  # Not in use ATM
    ALERTMANAGER_API_HOST = ('', str)

    # iSCSI management settings
    ISCSI_API_SSL_VERIFICATION = (True, bool)

    @staticmethod
    def has_default_value(name):
        return getattr(Settings, name, None) is None or \
               getattr(Settings, name) == getattr(Options, name)[0]


class SettingsMeta(type):
    def __getattr__(cls, attr):
        default, stype = getattr(Options, attr)
        if stype == bool and str(mgr.get_module_option(
                attr,
                default)).lower() == 'false':
            value = False
        else:
            value = stype(mgr.get_module_option(attr, default))
        return value

    def __setattr__(cls, attr, value):
        if not attr.startswith('_') and hasattr(Options, attr):
            mgr.set_module_option(attr, str(value))
        else:
            setattr(SettingsMeta, attr, value)

    def __delattr__(cls, attr):
        if not attr.startswith('_') and hasattr(Options, attr):
            mgr.set_module_option(attr, None)


# pylint: disable=no-init
@add_metaclass(SettingsMeta)
class Settings(object):
    pass


def _options_command_map():
    def filter_attr(member):
        return not inspect.isroutine(member)

    cmd_map = {}
    for option, value in inspect.getmembers(Options, filter_attr):
        if option.startswith('_'):
            continue
        key_get = 'dashboard get-{}'.format(option.lower().replace('_', '-'))
        key_set = 'dashboard set-{}'.format(option.lower().replace('_', '-'))
        key_reset = 'dashboard reset-{}'.format(option.lower().replace('_', '-'))
        cmd_map[key_get] = {'name': option, 'type': None}
        cmd_map[key_set] = {'name': option, 'type': value[1]}
        cmd_map[key_reset] = {'name': option, 'type': None}
    return cmd_map


_OPTIONS_COMMAND_MAP = _options_command_map()


def options_command_list():
    """
    This function generates a list of ``get`` and ``set`` commands
    for each declared configuration option in class ``Options``.
    """
    def py2ceph(pytype):
        if pytype == str:
            return 'CephString'
        elif pytype == int:
            return 'CephInt'
        return 'CephString'

    cmd_list = []
    for cmd, opt in _OPTIONS_COMMAND_MAP.items():
        if cmd.startswith('dashboard get'):
            cmd_list.append({
                'cmd': '{}'.format(cmd),
                'desc': 'Get the {} option value'.format(opt['name']),
                'perm': 'r'
            })
        elif cmd.startswith('dashboard set'):
            cmd_list.append({
                'cmd': '{} name=value,type={}'
                       .format(cmd, py2ceph(opt['type'])),
                'desc': 'Set the {} option value'.format(opt['name']),
                'perm': 'w'
            })
        elif cmd.startswith('dashboard reset'):
            desc = 'Reset the {} option to its default value'.format(
                opt['name'])
            cmd_list.append({
                'cmd': '{}'.format(cmd),
                'desc': desc,
                'perm': 'w'
            })

    return cmd_list


def options_schema_list():
    def filter_attr(member):
        return not inspect.isroutine(member)

    result = []
    for option, value in inspect.getmembers(Options, filter_attr):
        if option.startswith('_'):
            continue
        result.append({'name': option, 'default': value[0]})

    return result


def handle_option_command(cmd):
    if cmd['prefix'] not in _OPTIONS_COMMAND_MAP:
        return -errno.ENOSYS, '', "Command not found '{}'".format(cmd['prefix'])

    opt = _OPTIONS_COMMAND_MAP[cmd['prefix']]

    if cmd['prefix'].startswith('dashboard reset'):
        delattr(Settings, opt['name'])
        return 0, 'Option {} reset to default value "{}"'.format(
            opt['name'], getattr(Settings, opt['name'])), ''
    elif cmd['prefix'].startswith('dashboard get'):
        return 0, str(getattr(Settings, opt['name'])), ''
    elif cmd['prefix'].startswith('dashboard set'):
        value = opt['type'](cmd['value'])
        if opt['type'] == bool and cmd['value'].lower() == 'false':
            value = False
        setattr(Settings, opt['name'], value)
        return 0, 'Option {} updated'.format(opt['name']), ''
