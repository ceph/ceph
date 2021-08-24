# -*- coding: utf-8 -*-
import errno
import inspect
from ast import literal_eval
from typing import Any

from mgr_module import CLICheckNonemptyFileInput

from . import mgr


class Setting:
    """
    Setting representation that allows to set a default value and a list of allowed data types.
    :param default_value: The name of the bucket.
    :param types: a list consisting of the primary/preferred type and, optionally,
    secondary/legacy types for backward compatibility.
    """

    def __init__(self, default_value: Any, types: list):
        if not isinstance(types, list):
            raise ValueError('Setting types must be a list.')
        default_value_type = type(default_value)
        if default_value_type not in types:
            raise ValueError('Default value type not allowed.')
        self.default_value = default_value
        self.types = types

    def types_as_str(self):
        return ','.join([x.__name__ for x in self.types])

    def cast(self, value):
        for type_index, setting_type in enumerate(self.types):
            try:
                if setting_type.__name__ == 'bool' and str(value).lower() == 'false':
                    return False
                elif setting_type.__name__ == 'dict':
                    return literal_eval(value)
                return setting_type(value)
            except (SyntaxError, TypeError, ValueError) as error:
                if type_index == len(self.types) - 1:
                    raise error


class Options(object):
    """
    If you need to store some configuration value please add the config option
    name as a class attribute to this class.

    Example::

        GRAFANA_API_HOST = ('localhost', str)
        GRAFANA_API_PORT = (3000, int)
    """
    ENABLE_BROWSABLE_API = Setting(True, [bool])
    REST_REQUESTS_TIMEOUT = Setting(45, [int])

    # AUTHENTICATION ATTEMPTS
    ACCOUNT_LOCKOUT_ATTEMPTS = Setting(10, [int])

    # API auditing
    AUDIT_API_ENABLED = Setting(False, [bool])
    AUDIT_API_LOG_PAYLOAD = Setting(True, [bool])

    # RGW settings
    RGW_API_ACCESS_KEY = Setting('', [dict, str])
    RGW_API_SECRET_KEY = Setting('', [dict, str])
    RGW_API_ADMIN_RESOURCE = Setting('admin', [str])
    RGW_API_SSL_VERIFY = Setting(True, [bool])

    # Ceph Issue Tracker API Access Key
    ISSUE_TRACKER_API_KEY = Setting('', [str])

    # Grafana settings
    GRAFANA_API_URL = Setting('', [str])
    GRAFANA_FRONTEND_API_URL = Setting('', [str])
    GRAFANA_API_USERNAME = Setting('admin', [str])
    GRAFANA_API_PASSWORD = Setting('admin', [str])
    GRAFANA_API_SSL_VERIFY = Setting(True, [bool])
    GRAFANA_UPDATE_DASHBOARDS = Setting(False, [bool])

    # NFS Ganesha settings
    GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = Setting('', [str])

    # Prometheus settings
    PROMETHEUS_API_HOST = Setting('', [str])
    PROMETHEUS_API_SSL_VERIFY = Setting(True, [bool])
    ALERTMANAGER_API_HOST = Setting('', [str])
    ALERTMANAGER_API_SSL_VERIFY = Setting(True, [bool])

    # iSCSI management settings
    ISCSI_API_SSL_VERIFICATION = Setting(True, [bool])

    # user management settings
    # Time span of user passwords to expire in days.
    # The default value is '0' which means that user passwords are
    # never going to expire.
    USER_PWD_EXPIRATION_SPAN = Setting(0, [int])
    # warning levels to notify the user that the password is going
    # to expire soon
    USER_PWD_EXPIRATION_WARNING_1 = Setting(10, [int])
    USER_PWD_EXPIRATION_WARNING_2 = Setting(5, [int])

    # Password policy
    PWD_POLICY_ENABLED = Setting(True, [bool])
    # Individual checks
    PWD_POLICY_CHECK_LENGTH_ENABLED = Setting(True, [bool])
    PWD_POLICY_CHECK_OLDPWD_ENABLED = Setting(True, [bool])
    PWD_POLICY_CHECK_USERNAME_ENABLED = Setting(False, [bool])
    PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED = Setting(False, [bool])
    PWD_POLICY_CHECK_COMPLEXITY_ENABLED = Setting(False, [bool])
    PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED = Setting(False, [bool])
    PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED = Setting(False, [bool])
    # Settings
    PWD_POLICY_MIN_LENGTH = Setting(8, [int])
    PWD_POLICY_MIN_COMPLEXITY = Setting(10, [int])
    PWD_POLICY_EXCLUSION_LIST = Setting(','.join(['osd', 'host', 'dashboard', 'pool',
                                                  'block', 'nfs', 'ceph', 'monitors',
                                                  'gateway', 'logs', 'crush', 'maps']),
                                        [str])

    @staticmethod
    def has_default_value(name):
        return getattr(Settings, name, None) is None or \
            getattr(Settings, name) == getattr(Options, name).default_value


class SettingsMeta(type):
    def __getattr__(cls, attr):
        setting = getattr(Options, attr)
        return setting.cast(mgr.get_module_option(attr, setting.default_value))

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
    for option, setting in inspect.getmembers(Options, filter_attr):
        if option.startswith('_'):
            continue
        key_get = 'dashboard get-{}'.format(option.lower().replace('_', '-'))
        key_set = 'dashboard set-{}'.format(option.lower().replace('_', '-'))
        key_reset = 'dashboard reset-{}'.format(option.lower().replace('_', '-'))
        cmd_map[key_get] = {'name': option, 'type': None}
        cmd_map[key_set] = {'name': option, 'type': setting.types_as_str()}
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
            cmd_entry = {
                'cmd': '{} name=value,type={}'
                       .format(cmd, py2ceph(opt['type'])),
                'desc': 'Set the {} option value'.format(opt['name']),
                'perm': 'w'
            }
            if handles_secret(cmd):
                cmd_entry['cmd'] = cmd
                cmd_entry['desc'] = '{} read from -i <file>'.format(cmd_entry['desc'])
            cmd_list.append(cmd_entry)
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
    for option, setting in inspect.getmembers(Options, filter_attr):
        if option.startswith('_'):
            continue
        result.append({'name': option, 'default': setting.default_value,
                       'type': setting.types_as_str()})

    return result


def handle_option_command(cmd, inbuf):
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
        if handles_secret(cmd['prefix']):
            value, stdout, stderr = get_secret(inbuf=inbuf)
            if stderr:
                return value, stdout, stderr
        else:
            value = cmd['value']
        setting = getattr(Options, opt['name'])
        setattr(Settings, opt['name'], setting.cast(value))
        return 0, 'Option {} updated'.format(opt['name']), ''


def handles_secret(cmd: str) -> bool:
    return bool([cmd for secret_word in ['password', 'key'] if (secret_word in cmd)])


@CLICheckNonemptyFileInput(desc='password/secret')
def get_secret(inbuf=None):
    return inbuf, None, None
