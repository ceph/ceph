# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno
import inspect

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
    GRAFANA_API_SSL_VERIFY = (True, bool)
    GRAFANA_UPDATE_DASHBOARDS = (False, bool)

    # NFS Ganesha settings
    GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = ('', str)

    # Prometheus settings
    PROMETHEUS_API_HOST = ('', str)
    ALERTMANAGER_API_HOST = ('', str)

    # iSCSI management settings
    ISCSI_API_SSL_VERIFICATION = (True, bool)

    # user management settings
    # Time span of user passwords to expire in days.
    # The default value is '0' which means that user passwords are
    # never going to expire.
    USER_PWD_EXPIRATION_SPAN = (0, int)
    # warning levels to notify the user that the password is going
    # to expire soon
    USER_PWD_EXPIRATION_WARNING_1 = (10, int)
    USER_PWD_EXPIRATION_WARNING_2 = (5, int)

    # Password policy
    PWD_POLICY_ENABLED = (True, bool)
    # Individual checks
    PWD_POLICY_CHECK_LENGTH_ENABLED = (True, bool)
    PWD_POLICY_CHECK_OLDPWD_ENABLED = (True, bool)
    PWD_POLICY_CHECK_USERNAME_ENABLED = (False, bool)
    PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED = (False, bool)
    PWD_POLICY_CHECK_COMPLEXITY_ENABLED = (False, bool)
    PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED = (False, bool)
    PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED = (False, bool)
    # Settings
    PWD_POLICY_MIN_LENGTH = (8, int)
    PWD_POLICY_MIN_COMPLEXITY = (10, int)
    PWD_POLICY_EXCLUSION_LIST = (','.join(['osd', 'host',
                                           'dashboard', 'pool',
                                           'block', 'nfs',
                                           'ceph', 'monitors',
                                           'gateway', 'logs',
                                           'crush', 'maps']),
                                 str)

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
class Settings(object, metaclass=SettingsMeta):
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
        result.append({'name': option, 'default': value[0],
                       'type': value[1].__name__})

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
