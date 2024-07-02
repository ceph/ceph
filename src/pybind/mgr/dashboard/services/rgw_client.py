# -*- coding: utf-8 -*-
# pylint: disable=C0302
# pylint: disable=too-many-branches
# pylint: disable=too-many-lines

import ipaddress
import json
import logging
import os
import re
import xml.etree.ElementTree as ET  # noqa: N814
from enum import Enum
from subprocess import SubprocessError

from mgr_util import build_url, name_to_config_section

from .. import mgr
from ..awsauth import S3Auth
from ..exceptions import DashboardException
from ..rest_client import RequestException, RestClient
from ..settings import Settings
from ..tools import dict_contains_path, dict_get, json_str_to_object, str_to_bool
from .ceph_service import CephService

try:
    from typing import Any, Dict, List, Optional, Tuple, Union
except ImportError:
    pass  # For typing only

logger = logging.getLogger('rgw_client')

_SYNC_GROUP_ID = 'dashboard_admin_group'
_SYNC_FLOW_ID = 'dashboard_admin_flow'
_SYNC_PIPE_ID = 'dashboard_admin_pipe'


class NoRgwDaemonsException(Exception):
    def __init__(self):
        super().__init__('No RGW service is running.')


class NoCredentialsException(Exception):
    def __init__(self):
        super(NoCredentialsException, self).__init__(
            'No RGW credentials found, '
            'please consult the documentation on how to enable RGW for '
            'the dashboard.')


class RgwAdminException(Exception):
    pass


class RgwDaemon:
    """Simple representation of a daemon."""
    host: str
    name: str
    port: int
    ssl: bool
    realm_name: str
    zonegroup_name: str
    zonegroup_id: str
    zone_name: str


def _get_daemons() -> Dict[str, RgwDaemon]:
    """
    Retrieve RGW daemon info from MGR.
    """
    service_map = mgr.get('service_map')
    if not dict_contains_path(service_map, ['services', 'rgw', 'daemons']):
        raise NoRgwDaemonsException

    daemons = {}
    daemon_map = service_map['services']['rgw']['daemons']
    for key in daemon_map.keys():
        if dict_contains_path(daemon_map[key], ['metadata', 'frontend_config#0']):
            daemon = _determine_rgw_addr(daemon_map[key])
            daemon.name = daemon_map[key]['metadata']['id']
            daemon.realm_name = daemon_map[key]['metadata']['realm_name']
            daemon.zonegroup_name = daemon_map[key]['metadata']['zonegroup_name']
            daemon.zonegroup_id = daemon_map[key]['metadata']['zonegroup_id']
            daemon.zone_name = daemon_map[key]['metadata']['zone_name']
            daemons[daemon.name] = daemon
            logger.info('Found RGW daemon with configuration: host=%s, port=%d, ssl=%s',
                        daemon.host, daemon.port, str(daemon.ssl))
    if not daemons:
        raise NoRgwDaemonsException

    return daemons


def _determine_rgw_addr(daemon_info: Dict[str, Any]) -> RgwDaemon:
    """
    Parse RGW daemon info to determine the configured host (IP address) and port.
    """
    daemon = RgwDaemon()
    rgw_dns_name = CephService.send_command('mon', 'config get',
                                            who=name_to_config_section('rgw.' + daemon_info['metadata']['id']),  # noqa E501 #pylint: disable=line-too-long
                                            key='rgw_dns_name').rstrip()

    daemon.port, daemon.ssl = _parse_frontend_config(daemon_info['metadata']['frontend_config#0'])

    if rgw_dns_name:
        daemon.host = rgw_dns_name
    elif daemon.ssl:
        daemon.host = daemon_info['metadata']['hostname']
    else:
        daemon.host = _parse_addr(daemon_info['addr'])

    return daemon


def _parse_addr(value) -> str:
    """
    Get the IP address the RGW is running on.

    >>> _parse_addr('192.168.178.3:49774/1534999298')
    '192.168.178.3'

    >>> _parse_addr('[2001:db8:85a3::8a2e:370:7334]:49774/1534999298')
    '2001:db8:85a3::8a2e:370:7334'

    >>> _parse_addr('xyz')
    Traceback (most recent call last):
    ...
    LookupError: Failed to determine RGW address

    >>> _parse_addr('192.168.178.a:8080/123456789')
    Traceback (most recent call last):
    ...
    LookupError: Invalid RGW address '192.168.178.a' found

    >>> _parse_addr('[2001:0db8:1234]:443/123456789')
    Traceback (most recent call last):
    ...
    LookupError: Invalid RGW address '2001:0db8:1234' found

    >>> _parse_addr('2001:0db8::1234:49774/1534999298')
    Traceback (most recent call last):
    ...
    LookupError: Failed to determine RGW address

    :param value: The string to process. The syntax is '<HOST>:<PORT>/<NONCE>'.
    :type: str
    :raises LookupError if parsing fails to determine the IP address.
    :return: The IP address.
    :rtype: str
    """
    match = re.search(r'^(\[)?(?(1)([^\]]+)\]|([^:]+)):\d+/\d+?', value)
    if match:
        # IPv4:
        #   Group 0: 192.168.178.3:49774/1534999298
        #   Group 3: 192.168.178.3
        # IPv6:
        #   Group 0: [2001:db8:85a3::8a2e:370:7334]:49774/1534999298
        #   Group 1: [
        #   Group 2: 2001:db8:85a3::8a2e:370:7334
        addr = match.group(3) if match.group(3) else match.group(2)
        try:
            ipaddress.ip_address(addr)
            return addr
        except ValueError:
            raise LookupError('Invalid RGW address \'{}\' found'.format(addr))
    raise LookupError('Failed to determine RGW address')


def _parse_frontend_config(config) -> Tuple[int, bool]:
    """
    Get the port the RGW is running on. Due the complexity of the
    syntax not all variations are supported.

    If there are multiple (ssl_)ports/(ssl_)endpoints options, then
    the first found option will be returned.

    Get more details about the configuration syntax here:
    http://docs.ceph.com/en/latest/radosgw/frontends/
    https://civetweb.github.io/civetweb/UserManual.html

    :param config: The configuration string to parse.
    :type config: str
    :raises LookupError if parsing fails to determine the port.
    :return: A tuple containing the port number and the information
             whether SSL is used.
    :rtype: (int, boolean)
    """
    match = re.search(r'^(beast|civetweb)\s+.+$', config)
    if match:
        if match.group(1) == 'beast':
            match = re.search(r'(port|ssl_port|endpoint|ssl_endpoint)=(.+)',
                              config)
            if match:
                option_name = match.group(1)
                if option_name in ['port', 'ssl_port']:
                    match = re.search(r'(\d+)', match.group(2))
                    if match:
                        port = int(match.group(1))
                        ssl = option_name == 'ssl_port'
                        return port, ssl
                if option_name in ['endpoint', 'ssl_endpoint']:
                    match = re.search(r'([\d.]+|\[.+\])(:(\d+))?',
                                      match.group(2))  # type: ignore
                    if match:
                        port = int(match.group(3)) if \
                            match.group(2) is not None else 443 if \
                            option_name == 'ssl_endpoint' else \
                            80
                        ssl = option_name == 'ssl_endpoint'
                        return port, ssl
        if match.group(1) == 'civetweb':  # type: ignore
            match = re.search(r'port=(.*:)?(\d+)(s)?', config)
            if match:
                port = int(match.group(2))
                ssl = match.group(3) == 's'
                return port, ssl
    raise LookupError('Failed to determine RGW port from "{}"'.format(config))


def _parse_secrets(user: str, data: dict) -> Tuple[str, str]:
    for key in data.get('keys', []):
        if key.get('user') == user and data.get('system') in ['true', True]:
            access_key = key.get('access_key')
            secret_key = key.get('secret_key')
            return access_key, secret_key
    return '', ''


def _get_user_keys(user: str, realm: Optional[str] = None) -> Tuple[str, str]:
    access_key = ''
    secret_key = ''
    rgw_user_info_cmd = ['user', 'info', '--uid', user]
    cmd_realm_option = ['--rgw-realm', realm] if realm else []
    if realm:
        rgw_user_info_cmd += cmd_realm_option
    try:
        _, out, err = mgr.send_rgwadmin_command(rgw_user_info_cmd)
        if out:
            access_key, secret_key = _parse_secrets(user, out)
        if not access_key:
            rgw_create_user_cmd = [
                'user', 'create',
                '--uid', user,
                '--display-name', 'Ceph Dashboard',
                '--system',
            ] + cmd_realm_option
            _, out, err = mgr.send_rgwadmin_command(rgw_create_user_cmd)
            if out:
                access_key, secret_key = _parse_secrets(user, out)
        if not access_key:
            logger.error('Unable to create rgw user "%s": %s', user, err)
    except SubprocessError as error:
        logger.exception(error)

    return access_key, secret_key


def configure_rgw_credentials():
    logger.info('Configuring dashboard RGW credentials')
    user = 'dashboard'
    realms = []
    access_key = ''
    secret_key = ''
    try:
        _, out, err = mgr.send_rgwadmin_command(['realm', 'list'])
        if out:
            realms = out.get('realms', [])
        if err:
            logger.error('Unable to list RGW realms: %s', err)
        if realms:
            realm_access_keys = {}
            realm_secret_keys = {}
            for realm in realms:
                realm_access_key, realm_secret_key = _get_user_keys(user, realm)
                if realm_access_key:
                    realm_access_keys[realm] = realm_access_key
                    realm_secret_keys[realm] = realm_secret_key
            if realm_access_keys:
                access_key = json.dumps(realm_access_keys)
                secret_key = json.dumps(realm_secret_keys)
        else:
            access_key, secret_key = _get_user_keys(user)

        assert access_key and secret_key
        Settings.RGW_API_ACCESS_KEY = access_key
        Settings.RGW_API_SECRET_KEY = secret_key
    except (AssertionError, SubprocessError) as error:
        logger.exception(error)
        raise NoCredentialsException


# pylint: disable=R0904
class RgwClient(RestClient):
    _host = None
    _port = None
    _ssl = None
    _user_instances = {}  # type: Dict[str, Dict[str, RgwClient]]
    _config_instances = {}  # type: Dict[str, RgwClient]
    _rgw_settings_snapshot = None
    _daemons: Dict[str, RgwDaemon] = {}
    daemon: RgwDaemon
    got_keys_from_config: bool
    userid: str

    @staticmethod
    def _handle_response_status_code(status_code: int) -> int:
        # Do not return auth error codes (so they are not handled as ceph API user auth errors).
        return 404 if status_code in [401, 403] else status_code

    @staticmethod
    def _get_daemon_connection_info(daemon_name: str) -> dict:
        try:
            realm_name = RgwClient._daemons[daemon_name].realm_name
            access_key = Settings.RGW_API_ACCESS_KEY[realm_name]
            secret_key = Settings.RGW_API_SECRET_KEY[realm_name]
        except TypeError:
            # Legacy string values.
            access_key = Settings.RGW_API_ACCESS_KEY
            secret_key = Settings.RGW_API_SECRET_KEY
        except KeyError as error:
            raise DashboardException(msg='Credentials not found for RGW Daemon: {}'.format(error),
                                     http_status_code=404,
                                     component='rgw')

        return {'access_key': access_key, 'secret_key': secret_key}

    def _get_daemon_zone_info(self):  # type: () -> dict
        return json_str_to_object(self.proxy('GET', 'config?type=zone', None, None))

    def _get_realms_info(self):  # type: () -> dict
        return json_str_to_object(self.proxy('GET', 'realm?list', None, None))

    def _get_realm_info(self, realm_id: str) -> Dict[str, Any]:
        return json_str_to_object(self.proxy('GET', f'realm?id={realm_id}', None, None))

    @staticmethod
    def _rgw_settings():
        return (Settings.RGW_API_ACCESS_KEY,
                Settings.RGW_API_SECRET_KEY,
                Settings.RGW_API_ADMIN_RESOURCE,
                Settings.RGW_API_SSL_VERIFY)

    @staticmethod
    def instance(userid: Optional[str] = None,
                 daemon_name: Optional[str] = None) -> 'RgwClient':
        # pylint: disable=too-many-branches

        RgwClient._daemons = _get_daemons()

        # The API access key and secret key are mandatory for a minimal configuration.
        if not (Settings.RGW_API_ACCESS_KEY and Settings.RGW_API_SECRET_KEY):
            configure_rgw_credentials()

        daemon_keys = RgwClient._daemons.keys()
        if not daemon_name:
            if len(daemon_keys) > 1:
                try:
                    multiiste = RgwMultisite()
                    default_zonegroup = multiiste.get_all_zonegroups_info()['default_zonegroup']

                    # Iterate through _daemons.values() to find the daemon with the
                    # matching zonegroup_id
                    for daemon in RgwClient._daemons.values():
                        if daemon.zonegroup_id == default_zonegroup:
                            daemon_name = daemon.name
                            break
                except Exception:  # pylint: disable=broad-except
                    daemon_name = next(iter(daemon_keys))
            else:
                # Handle the case where there is only one or no key in _daemons
                daemon_name = next(iter(daemon_keys))

        # Discard all cached instances if any rgw setting has changed
        if RgwClient._rgw_settings_snapshot != RgwClient._rgw_settings():
            RgwClient._rgw_settings_snapshot = RgwClient._rgw_settings()
            RgwClient.drop_instance()

        if daemon_name not in RgwClient._config_instances:
            connection_info = RgwClient._get_daemon_connection_info(daemon_name)  # type: ignore
            RgwClient._config_instances[daemon_name] = RgwClient(connection_info['access_key'],  # type: ignore  # noqa E501  #pylint: disable=line-too-long
                                                                 connection_info['secret_key'],
                                                                 daemon_name)  # type: ignore

        if not userid or userid == RgwClient._config_instances[daemon_name].userid:  # type: ignore
            return RgwClient._config_instances[daemon_name]  # type: ignore

        if daemon_name not in RgwClient._user_instances \
                or userid not in RgwClient._user_instances[daemon_name]:
            # Get the access and secret keys for the specified user.
            keys = RgwClient._config_instances[daemon_name].get_user_keys(userid)  # type: ignore
            if not keys:
                raise RequestException(
                    "User '{}' does not have any keys configured.".format(
                        userid))
            instance = RgwClient(keys['access_key'],
                                 keys['secret_key'],
                                 daemon_name,  # type: ignore
                                 userid)
            RgwClient._user_instances.update({daemon_name: {userid: instance}})  # type: ignore

        return RgwClient._user_instances[daemon_name][userid]  # type: ignore

    @staticmethod
    def admin_instance(daemon_name: Optional[str] = None) -> 'RgwClient':
        return RgwClient.instance(daemon_name=daemon_name)

    @staticmethod
    def drop_instance(instance: Optional['RgwClient'] = None):
        """
        Drop a cached instance or all.
        """
        if instance:
            if instance.got_keys_from_config:
                del RgwClient._config_instances[instance.daemon.name]
            else:
                del RgwClient._user_instances[instance.daemon.name][instance.userid]
        else:
            RgwClient._config_instances.clear()
            RgwClient._user_instances.clear()

    def _reset_login(self):
        if self.got_keys_from_config:
            raise RequestException('Authentication failed for the "{}" user: wrong credentials'
                                   .format(self.userid), status_code=401)
        logger.info("Fetching new keys for user: %s", self.userid)
        keys = RgwClient.admin_instance(daemon_name=self.daemon.name).get_user_keys(self.userid)
        self.auth = S3Auth(keys['access_key'], keys['secret_key'],
                           service_url=self.service_url)

    def __init__(self,
                 access_key: str,
                 secret_key: str,
                 daemon_name: str,
                 user_id: Optional[str] = None) -> None:
        try:
            daemon = RgwClient._daemons[daemon_name]
        except KeyError as error:
            raise DashboardException(msg='RGW Daemon not found: {}'.format(error),
                                     http_status_code=404,
                                     component='rgw')
        ssl_verify = Settings.RGW_API_SSL_VERIFY
        self.admin_path = Settings.RGW_API_ADMIN_RESOURCE
        self.service_url = build_url(host=daemon.host, port=daemon.port)

        self.auth = S3Auth(access_key, secret_key, service_url=self.service_url)
        super(RgwClient, self).__init__(daemon.host,
                                        daemon.port,
                                        'RGW',
                                        daemon.ssl,
                                        self.auth,
                                        ssl_verify=ssl_verify)
        self.got_keys_from_config = not user_id
        try:
            self.userid = self._get_user_id(self.admin_path) if self.got_keys_from_config \
                else user_id
        except RequestException as error:
            logger.exception(error)
            msg = 'Error connecting to Object Gateway'
            if error.status_code == 404:
                msg = '{}: {}'.format(msg, str(error))
            raise DashboardException(msg=msg,
                                     http_status_code=error.status_code,
                                     component='rgw')
        self.daemon = daemon

        logger.info("Created new connection: daemon=%s, host=%s, port=%s, ssl=%d, sslverify=%d",
                    daemon.name, daemon.host, daemon.port, daemon.ssl, ssl_verify)

    @RestClient.api_get('/', resp_structure='[0] > (ID & DisplayName)')
    def is_service_online(self, request=None) -> bool:
        """
        Consider the service as online if the response contains the
        specified keys. Nothing more is checked here.
        """
        _ = request({'format': 'json'})
        return True

    @RestClient.api_get('/{admin_path}/metadata/user?myself',
                        resp_structure='data > user_id')
    def _get_user_id(self, admin_path, request=None):
        # pylint: disable=unused-argument
        """
        Get the user ID of the user that is used to communicate with the
        RGW Admin Ops API.
        :rtype: str
        :return: The user ID of the user that is used to sign the
                 RGW Admin Ops API calls.
        """
        response = request()
        return response['data']['user_id']

    @RestClient.api_get('/{admin_path}/metadata/user', resp_structure='[+]')
    def _user_exists(self, admin_path, user_id, request=None):
        # pylint: disable=unused-argument
        response = request()
        if user_id:
            return user_id in response
        return self.userid in response

    def user_exists(self, user_id=None):
        return self._user_exists(self.admin_path, user_id)

    @RestClient.api_get('/{admin_path}/metadata/user?key={userid}',
                        resp_structure='data > system')
    def _is_system_user(self, admin_path, userid, request=None) -> bool:
        # pylint: disable=unused-argument
        response = request()
        return response['data']['system']

    def is_system_user(self) -> bool:
        return self._is_system_user(self.admin_path, self.userid)

    @RestClient.api_get(
        '/{admin_path}/user',
        resp_structure='tenant & user_id & email & keys[*] > '
        ' (user & access_key & secret_key)')
    def _admin_get_user_keys(self, admin_path, userid, request=None):
        # pylint: disable=unused-argument
        colon_idx = userid.find(':')
        user = userid if colon_idx == -1 else userid[:colon_idx]
        response = request({'uid': user})
        for key in response['keys']:
            if key['user'] == userid:
                return {
                    'access_key': key['access_key'],
                    'secret_key': key['secret_key']
                }
        return None

    def get_user_keys(self, userid):
        return self._admin_get_user_keys(self.admin_path, userid)

    @RestClient.api('/{admin_path}/{path}')
    def _proxy_request(
            self,  # pylint: disable=too-many-arguments
            admin_path,
            path,
            method,
            params,
            data,
            request=None):
        # pylint: disable=unused-argument
        return request(method=method, params=params, data=data,
                       raw_content=True)

    def proxy(self, method, path, params, data):
        logger.debug("proxying method=%s path=%s params=%s data=%s",
                     method, path, params, data)
        return self._proxy_request(self.admin_path, path, method,
                                   params, data)

    @RestClient.api_get('/', resp_structure='[1][*] > Name')
    def get_buckets(self, request=None):
        """
        Get a list of names from all existing buckets of this user.
        :return: Returns a list of bucket names.
        """
        response = request({'format': 'json'})
        return [bucket['Name'] for bucket in response[1]]

    @RestClient.api_get('/{bucket_name}')
    def bucket_exists(self, bucket_name, userid, request=None):
        """
        Check if the specified bucket exists for this user.
        :param bucket_name: The name of the bucket.
        :return: Returns True if the bucket exists, otherwise False.
        """
        # pylint: disable=unused-argument
        try:
            request()
            my_buckets = self.get_buckets()
            if bucket_name not in my_buckets:
                raise RequestException(
                    'Bucket "{}" belongs to other user'.format(bucket_name),
                    403)
            return True
        except RequestException as e:
            if e.status_code == 404:
                return False

            raise e

    @RestClient.api_put('/{bucket_name}')
    def create_bucket(self, bucket_name, zonegroup=None,
                      placement_target=None, lock_enabled=False,
                      request=None):
        logger.info("Creating bucket: %s, zonegroup: %s, placement_target: %s",
                    bucket_name, zonegroup, placement_target)
        data = None
        if zonegroup and placement_target:
            create_bucket_configuration = ET.Element('CreateBucketConfiguration')
            location_constraint = ET.SubElement(create_bucket_configuration, 'LocationConstraint')
            location_constraint.text = '{}:{}'.format(zonegroup, placement_target)
            data = ET.tostring(create_bucket_configuration, encoding='unicode')

        headers = None  # type: Optional[dict]
        if lock_enabled:
            headers = {'x-amz-bucket-object-lock-enabled': 'true'}

        return request(data=data, headers=headers)

    def get_placement_targets(self):  # type: () -> dict
        zone = self._get_daemon_zone_info()
        placement_targets = []  # type: List[Dict]
        for placement_pool in zone['placement_pools']:
            placement_targets.append(
                {
                    'name': placement_pool['key'],
                    'data_pool': placement_pool['val']['storage_classes']['STANDARD']['data_pool']
                }
            )

        return {'zonegroup': self.daemon.zonegroup_name,
                'placement_targets': placement_targets}

    def get_realms(self):  # type: () -> List
        realms_info = self._get_realms_info()
        if 'realms' in realms_info and realms_info['realms']:
            return realms_info['realms']
        return []

    def get_default_realm(self):
        realms_info = self._get_realms_info()
        if 'default_info' in realms_info and realms_info['default_info']:
            realm_info = self._get_realm_info(realms_info['default_info'])
            if 'name' in realm_info and realm_info['name']:
                return realm_info['name']
        return None

    def get_default_zonegroup(self):
        return self.daemon.zonegroup_name

    @RestClient.api_get('/{bucket_name}?versioning')
    def get_bucket_versioning(self, bucket_name, request=None):
        """
        Get bucket versioning.
        :param str bucket_name: the name of the bucket.
        :return: versioning info
        :rtype: Dict
        """
        # pylint: disable=unused-argument
        result = request()
        if 'Status' not in result:
            result['Status'] = 'Suspended'
        if 'MfaDelete' not in result:
            result['MfaDelete'] = 'Disabled'
        return result

    @RestClient.api_put('/{bucket_name}?versioning')
    def set_bucket_versioning(self, bucket_name, versioning_state, mfa_delete,
                              mfa_token_serial, mfa_token_pin, request=None):
        """
        Set bucket versioning.
        :param str bucket_name: the name of the bucket.
        :param str versioning_state:
            https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html
        :param str mfa_delete: MFA Delete state.
        :param str mfa_token_serial:
            https://docs.ceph.com/docs/master/radosgw/mfa/
        :param str mfa_token_pin: value of a TOTP token at a certain time (auth code)
        :return: None
        """
        # pylint: disable=unused-argument
        versioning_configuration = ET.Element('VersioningConfiguration')
        status_element = ET.SubElement(versioning_configuration, 'Status')
        status_element.text = versioning_state

        headers = {}
        if mfa_delete and mfa_token_serial and mfa_token_pin:
            headers['x-amz-mfa'] = '{} {}'.format(mfa_token_serial, mfa_token_pin)
            mfa_delete_element = ET.SubElement(versioning_configuration, 'MfaDelete')
            mfa_delete_element.text = mfa_delete

        data = ET.tostring(versioning_configuration, encoding='unicode')

        try:
            request(data=data, headers=headers)
        except RequestException as error:
            msg = str(error)
            if mfa_delete and mfa_token_serial and mfa_token_pin \
                    and 'AccessDenied' in error.content.decode():
                msg = 'Bad MFA credentials: {}'.format(msg)
            raise DashboardException(msg=msg,
                                     http_status_code=error.status_code,
                                     component='rgw')

    @RestClient.api_get('/{bucket_name}?acl')
    def get_acl(self, bucket_name, request=None):
        # pylint: disable=unused-argument
        try:
            result = request(raw_content=True)  # type: ignore
            return result.decode("utf-8")
        except RequestException as error:
            msg = 'Error getting ACLs'
            if error.status_code == 404:
                msg = '{}: {}'.format(msg, str(error))
            raise DashboardException(msg=msg,
                                     http_status_code=error.status_code,
                                     component='rgw')

    @RestClient.api_put('/{bucket_name}?acl')
    def set_acl(self, bucket_name, acl, request=None):
        # pylint: disable=unused-argument
        headers = {'x-amz-acl': acl}
        try:
            result = request(headers=headers)  # type: ignore
        except RequestException as e:
            raise DashboardException(msg=str(e), component='rgw')
        return result

    @RestClient.api_get('/{bucket_name}?encryption')
    def get_bucket_encryption(self, bucket_name, request=None):
        # pylint: disable=unused-argument
        try:
            result = request()  # type: ignore
            result['Status'] = 'Enabled'
            return result
        except RequestException as e:
            if e.content:
                content = json_str_to_object(e.content)
                if content.get(
                        'Code') == 'ServerSideEncryptionConfigurationNotFoundError':
                    return {
                        'Status': 'Disabled',
                    }
            raise e

    @RestClient.api_delete('/{bucket_name}?encryption')
    def delete_bucket_encryption(self, bucket_name, request=None):
        # pylint: disable=unused-argument
        result = request()  # type: ignore
        return result

    @RestClient.api_put('/{bucket_name}?encryption')
    def set_bucket_encryption(self, bucket_name, key_id,
                              sse_algorithm, request: Optional[object] = None):
        # pylint: disable=unused-argument
        encryption_configuration = ET.Element('ServerSideEncryptionConfiguration')
        rule_element = ET.SubElement(encryption_configuration, 'Rule')
        default_encryption_element = ET.SubElement(rule_element,
                                                   'ApplyServerSideEncryptionByDefault')
        sse_algo_element = ET.SubElement(default_encryption_element,
                                         'SSEAlgorithm')
        sse_algo_element.text = sse_algorithm
        if sse_algorithm == 'aws:kms':
            kms_master_key_element = ET.SubElement(default_encryption_element,
                                                   'KMSMasterKeyID')
            kms_master_key_element.text = key_id
        data = ET.tostring(encryption_configuration, encoding='unicode')
        try:
            _ = request(data=data)  # type: ignore
        except RequestException as e:
            raise DashboardException(msg=str(e), component='rgw')

    @RestClient.api_put('/{bucket_name}?tagging')
    def set_tags(self, bucket_name, tags, request=None):
        # pylint: disable=unused-argument
        try:
            ET.fromstring(tags)
        except ET.ParseError:
            return "Data must be properly formatted"
        try:
            result = request(data=tags)  # type: ignore
        except RequestException as e:
            raise DashboardException(msg=str(e), component='rgw')
        return result

    @RestClient.api_get('/{bucket_name}?object-lock')
    def get_bucket_locking(self, bucket_name, request=None):
        # type: (str, Optional[object]) -> dict
        """
        Gets the locking configuration for a bucket. The locking
        configuration will be applied by default to every new object
        placed in the specified bucket.
        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :return: The locking configuration.
        :rtype: Dict
        """
        # pylint: disable=unused-argument

        # Try to get the Object Lock configuration. If there is none,
        # then return default values.
        try:
            result = request()  # type: ignore
            return {
                'lock_enabled': dict_get(result, 'ObjectLockEnabled') == 'Enabled',
                'lock_mode': dict_get(result, 'Rule.DefaultRetention.Mode'),
                'lock_retention_period_days': dict_get(result, 'Rule.DefaultRetention.Days', 0),
                'lock_retention_period_years': dict_get(result, 'Rule.DefaultRetention.Years', 0)
            }
        except RequestException as e:
            if e.content:
                content = json_str_to_object(e.content)
                if content.get(
                        'Code') == 'ObjectLockConfigurationNotFoundError':
                    return {
                        'lock_enabled': False,
                        'lock_mode': 'compliance',
                        'lock_retention_period_days': None,
                        'lock_retention_period_years': None
                    }
            raise e

    @RestClient.api_put('/{bucket_name}?object-lock')
    def set_bucket_locking(self,
                           bucket_name: str,
                           mode: str,
                           retention_period_days: Optional[Union[int, str]] = None,
                           retention_period_years: Optional[Union[int, str]] = None,
                           request: Optional[object] = None) -> None:
        """
        Places the locking configuration on the specified bucket. The
        locking configuration will be applied by default to every new
        object placed in the specified bucket.
        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :param mode: The lock mode, e.g. `COMPLIANCE` or `GOVERNANCE`.
        :type mode: str
        :param retention_period_days:
        :type retention_period_days: int
        :param retention_period_years:
        :type retention_period_years: int
        :rtype: None
        """
        # pylint: disable=unused-argument

        retention_period_days, retention_period_years = self.perform_validations(
            retention_period_days, retention_period_years, mode)

        # Generate the XML data like this:
        # <ObjectLockConfiguration>
        #    <ObjectLockEnabled>string</ObjectLockEnabled>
        #    <Rule>
        #       <DefaultRetention>
        #          <Days>integer</Days>
        #          <Mode>string</Mode>
        #          <Years>integer</Years>
        #       </DefaultRetention>
        #    </Rule>
        # </ObjectLockConfiguration>
        locking_configuration = ET.Element('ObjectLockConfiguration')
        enabled_element = ET.SubElement(locking_configuration,
                                        'ObjectLockEnabled')
        enabled_element.text = 'Enabled'  # Locking can't be disabled.
        rule_element = ET.SubElement(locking_configuration, 'Rule')
        default_retention_element = ET.SubElement(rule_element,
                                                  'DefaultRetention')
        mode_element = ET.SubElement(default_retention_element, 'Mode')
        mode_element.text = mode.upper()
        if retention_period_days:
            days_element = ET.SubElement(default_retention_element, 'Days')
            days_element.text = str(retention_period_days)
        if retention_period_years:
            years_element = ET.SubElement(default_retention_element, 'Years')
            years_element.text = str(retention_period_years)

        data = ET.tostring(locking_configuration, encoding='unicode')

        try:
            _ = request(data=data)  # type: ignore
        except RequestException as e:
            raise DashboardException(msg=str(e), component='rgw')

    def list_roles(self) -> List[Dict[str, Any]]:
        rgw_list_roles_command = ['role', 'list']
        code, roles, err = mgr.send_rgwadmin_command(rgw_list_roles_command)
        if code < 0:
            logger.warning('Error listing roles with code %d: %s', code, err)
            return []

        for role in roles:
            if 'PermissionPolicies' not in role:
                role['PermissionPolicies'] = []
        return roles

    def create_role(self, role_name: str, role_path: str, role_assume_policy_doc: str) -> None:
        try:
            json.loads(role_assume_policy_doc)
        except:  # noqa: E722
            raise DashboardException('Assume role policy document is not a valid json')

        # valid values:
        # pylint: disable=C0301
        # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-role.html#cfn-iam-role-path # noqa: E501
        if len(role_name) > 64:
            raise DashboardException(
                f'Role name "{role_name}" is invalid. Should be 64 characters or less')

        role_name_regex = '[0-9a-zA-Z_+=,.@-]+'
        if not re.fullmatch(role_name_regex, role_name):
            raise DashboardException(
                f'Role name "{role_name}" is invalid. Valid characters are "{role_name_regex}"')

        if not os.path.isabs(role_path):
            raise DashboardException(
                f'Role path "{role_path}" is invalid. It should be an absolute path')
        if role_path[-1] != '/':
            raise DashboardException(
                f'Role path "{role_path}" is invalid. It should start and end with a slash')
        path_regex = '(\u002F)|(\u002F[\u0021-\u007E]+\u002F)'
        if not re.fullmatch(path_regex, role_path):
            raise DashboardException(
                (f'Role path "{role_path}" is invalid.'
                 f'Role path should follow the pattern "{path_regex}"'))

        rgw_create_role_command = ['role', 'create', '--role-name', role_name, '--path', role_path]
        if role_assume_policy_doc:
            rgw_create_role_command += ['--assume-role-policy-doc', f"{role_assume_policy_doc}"]

        code, _roles, _err = mgr.send_rgwadmin_command(rgw_create_role_command,
                                                       stdout_as_json=False)
        if code != 0:
            # pylint: disable=C0301
            link = 'https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-role.html#cfn-iam-role-path'  # noqa: E501
            msg = (f'Error creating role with code {code}: '
                   'Looks like the document has a wrong format.'
                   f' For more information about the format look at {link}')
            raise DashboardException(msg=msg, component='rgw')

    def get_role(self, role_name: str):
        rgw_get_role_command = ['role', 'get', '--role-name', role_name]
        code, role, _err = mgr.send_rgwadmin_command(rgw_get_role_command)
        if code != 0:
            raise DashboardException(msg=f'Error getting role with code {code}: {_err}',
                                     component='rgw')
        return role

    def update_role(self, role_name: str, max_session_duration: str):
        rgw_update_role_command = ['role', 'update', '--role-name',
                                   role_name, '--max_session_duration', max_session_duration]
        code, _, _err = mgr.send_rgwadmin_command(rgw_update_role_command,
                                                  stdout_as_json=False)
        if code != 0:
            raise DashboardException(msg=f'Error updating role with code {code}: {_err}',
                                     component='rgw')

    def delete_role(self, role_name: str) -> None:
        rgw_delete_role_command = ['role', 'delete', '--role-name', role_name]
        code, _, _err = mgr.send_rgwadmin_command(rgw_delete_role_command,
                                                  stdout_as_json=False)
        if code != 0:
            raise DashboardException(msg=f'Error deleting role with code {code}: {_err}',
                                     component='rgw')

    @RestClient.api_get('/{bucket_name}?policy')
    def get_bucket_policy(self, bucket_name: str, request=None):
        """
        Gets the bucket policy for a bucket.
        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :rtype: None
        """
        # pylint: disable=unused-argument

        try:
            request = request()
            return request
        except RequestException as e:
            if e.content:
                content = json_str_to_object(e.content)
                if content.get(
                        'Code') == 'NoSuchBucketPolicy':
                    return None
            raise e

    @RestClient.api_put('/{bucket_name}?policy')
    def set_bucket_policy(self, bucket_name: str, policy: str, request=None):
        """
        Sets the bucket policy for a bucket.
        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :param policy: The bucket policy.
        :type policy: JSON Structured Document
        :return: The bucket policy.
        :rtype: Dict
        """
        # pylint: disable=unused-argument
        try:
            request = request(data=policy)
        except RequestException as e:
            if e.content:
                content = json_str_to_object(e.content)
                if content.get("Code") == "InvalidArgument":
                    msg = "Invalid JSON document"
                    raise DashboardException(msg=msg, component='rgw')
            raise DashboardException(e)

    def perform_validations(self, retention_period_days, retention_period_years, mode):
        try:
            retention_period_days = int(retention_period_days) if retention_period_days else 0
            retention_period_years = int(retention_period_years) if retention_period_years else 0
            if retention_period_days < 0 or retention_period_years < 0:
                raise ValueError
        except (TypeError, ValueError):
            msg = "Retention period must be a positive integer."
            raise DashboardException(msg=msg, component='rgw')
        if retention_period_days and retention_period_years:
            # https://docs.aws.amazon.com/AmazonS3/latest/API/archive-RESTBucketPUTObjectLockConfiguration.html
            msg = "Retention period requires either Days or Years. "\
                "You can't specify both at the same time."
            raise DashboardException(msg=msg, component='rgw')
        if not retention_period_days and not retention_period_years:
            msg = "Retention period requires either Days or Years. "\
                "You must specify at least one."
            raise DashboardException(msg=msg, component='rgw')
        if not isinstance(mode, str) or mode.upper() not in ['COMPLIANCE', 'GOVERNANCE']:
            msg = "Retention mode must be either COMPLIANCE or GOVERNANCE."
            raise DashboardException(msg=msg, component='rgw')
        return retention_period_days, retention_period_years

    @RestClient.api_put('/{bucket_name}?replication')
    def set_bucket_replication(self, bucket_name, replication: bool, request=None):
        # pGenerate the minimum replication configuration
        # required for enabling the replication
        root = ET.Element('ReplicationConfiguration',
                          xmlns='http://s3.amazonaws.com/doc/2006-03-01/')
        role = ET.SubElement(root, 'Role')
        role.text = f'{bucket_name}_replication_role'

        rule = ET.SubElement(root, 'Rule')
        rule_id = ET.SubElement(rule, 'ID')
        rule_id.text = _SYNC_PIPE_ID

        status = ET.SubElement(rule, 'Status')
        status.text = 'Enabled' if replication else 'Disabled'

        filter_elem = ET.SubElement(rule, 'Filter')
        prefix = ET.SubElement(filter_elem, 'Prefix')
        prefix.text = ''

        destination = ET.SubElement(rule, 'Destination')

        bucket = ET.SubElement(destination, 'Bucket')
        bucket.text = bucket_name

        replication_config = ET.tostring(root, encoding='utf-8', method='xml').decode()

        try:
            request = request(data=replication_config)
        except RequestException as e:
            raise DashboardException(msg=str(e), component='rgw')

    @RestClient.api_get('/{bucket_name}?replication')
    def get_bucket_replication(self, bucket_name, request=None):
        # pylint: disable=unused-argument
        try:
            result = request()
            return result
        except RequestException as e:
            if e.content:
                content = json_str_to_object(e.content)
                if content.get('Code') == 'ReplicationConfigurationNotFoundError':
                    return None
            raise e


class SyncStatus(Enum):
    enabled = 'enabled'
    allowed = 'allowed'
    forbidden = 'forbidden'


class SyncFlowTypes(Enum):
    directional = 'directional'
    symmetrical = 'symmetrical'


class RgwMultisite:
    def migrate_to_multisite(self, realm_name: str, zonegroup_name: str, zone_name: str,
                             zonegroup_endpoints: str, zone_endpoints: str, access_key: str,
                             secret_key: str):
        rgw_realm_create_cmd = ['realm', 'create', '--rgw-realm', realm_name, '--default']
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_realm_create_cmd, False)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to create realm',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        rgw_zonegroup_edit_cmd = ['zonegroup', 'rename', '--rgw-zonegroup', 'default',
                                  '--zonegroup-new-name', zonegroup_name]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zonegroup_edit_cmd, False)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to rename zonegroup to {}'.format(zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        rgw_zone_edit_cmd = ['zone', 'rename', '--rgw-zone',
                             'default', '--zone-new-name', zone_name,
                             '--rgw-zonegroup', zonegroup_name]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_edit_cmd, False)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to rename zone to {}'.format(zone_name),  # noqa E501 #pylint: disable=line-too-long
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        rgw_zonegroup_modify_cmd = ['zonegroup', 'modify',
                                    '--rgw-realm', realm_name,
                                    '--rgw-zonegroup', zonegroup_name]
        if zonegroup_endpoints:
            rgw_zonegroup_modify_cmd.append('--endpoints')
            rgw_zonegroup_modify_cmd.append(zonegroup_endpoints)
        rgw_zonegroup_modify_cmd.append('--master')
        rgw_zonegroup_modify_cmd.append('--default')
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zonegroup_modify_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to modify zonegroup {}'.format(zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        rgw_zone_modify_cmd = ['zone', 'modify', '--rgw-realm', realm_name,
                               '--rgw-zonegroup', zonegroup_name,
                               '--rgw-zone', zone_name]
        if zone_endpoints:
            rgw_zone_modify_cmd.append('--endpoints')
            rgw_zone_modify_cmd.append(zone_endpoints)
        rgw_zone_modify_cmd.append('--master')
        rgw_zone_modify_cmd.append('--default')
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_modify_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to modify zone',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        if access_key and secret_key:
            rgw_zone_modify_cmd = ['zone', 'modify', '--rgw-zone', zone_name,
                                   '--access-key', access_key, '--secret', secret_key]
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_modify_cmd)
                if exit_code > 0:
                    raise DashboardException(e=err, msg='Unable to modify zone',
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')

    def create_realm(self, realm_name: str, default: bool):
        rgw_realm_create_cmd = ['realm', 'create']
        cmd_create_realm_options = ['--rgw-realm', realm_name]
        if default:
            cmd_create_realm_options.append('--default')
        rgw_realm_create_cmd += cmd_create_realm_options
        try:
            exit_code, _, _ = mgr.send_rgwadmin_command(rgw_realm_create_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to create realm',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def list_realms(self):
        rgw_realm_list = {}
        rgw_realm_list_cmd = ['realm', 'list']
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_realm_list_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to fetch realm list',
                                         http_status_code=500, component='rgw')
            rgw_realm_list = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return rgw_realm_list

    def get_realm(self, realm_name: str):
        realm_info = {}
        rgw_realm_info_cmd = ['realm', 'get', '--rgw-realm', realm_name]
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_realm_info_cmd)
            if exit_code > 0:
                raise DashboardException('Unable to get realm info',
                                         http_status_code=500, component='rgw')
            realm_info = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return realm_info

    def get_all_realms_info(self):
        all_realms_info = {}
        realms_info = []
        rgw_realm_list = self.list_realms()
        if 'realms' in rgw_realm_list:
            if rgw_realm_list['realms'] != []:
                for rgw_realm in rgw_realm_list['realms']:
                    realm_info = self.get_realm(rgw_realm)
                    realms_info.append(realm_info)
                    all_realms_info['realms'] = realms_info  # type: ignore
            else:
                all_realms_info['realms'] = []  # type: ignore
        if 'default_info' in rgw_realm_list and rgw_realm_list['default_info'] != '':
            all_realms_info['default_realm'] = rgw_realm_list['default_info']  # type: ignore
        else:
            all_realms_info['default_realm'] = ''  # type: ignore
        return all_realms_info

    def edit_realm(self, realm_name: str, new_realm_name: str, default: str = ''):
        rgw_realm_edit_cmd = []
        if new_realm_name != realm_name:
            rgw_realm_edit_cmd = ['realm', 'rename', '--rgw-realm',
                                  realm_name, '--realm-new-name', new_realm_name]
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_realm_edit_cmd, False)
                if exit_code > 0:
                    raise DashboardException(e=err, msg='Unable to edit realm',
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
        if default and str_to_bool(default):
            rgw_realm_edit_cmd = ['realm', 'default', '--rgw-realm', new_realm_name]
            try:
                exit_code, _, _ = mgr.send_rgwadmin_command(rgw_realm_edit_cmd, False)
                if exit_code > 0:
                    raise DashboardException(msg='Unable to set {} as default realm'.format(new_realm_name),  # noqa E501  #pylint: disable=line-too-long
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')

    def delete_realm(self, realm_name: str):
        rgw_delete_realm_cmd = ['realm', 'rm', '--rgw-realm', realm_name]
        try:
            exit_code, _, _ = mgr.send_rgwadmin_command(rgw_delete_realm_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to delete realm',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def create_zonegroup(self, realm_name: str, zonegroup_name: str,
                         default: bool, master: bool, endpoints: str):
        rgw_zonegroup_create_cmd = ['zonegroup', 'create']
        cmd_create_zonegroup_options = ['--rgw-zonegroup', zonegroup_name]
        if realm_name != 'null':
            cmd_create_zonegroup_options.append('--rgw-realm')
            cmd_create_zonegroup_options.append(realm_name)
        if default != 'false':
            cmd_create_zonegroup_options.append('--default')
        if master != 'false':
            cmd_create_zonegroup_options.append('--master')
        if endpoints:
            cmd_create_zonegroup_options.append('--endpoints')
            cmd_create_zonegroup_options.append(endpoints)
        rgw_zonegroup_create_cmd += cmd_create_zonegroup_options
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(rgw_zonegroup_create_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to get realm info',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return out

    def list_zonegroups(self):
        rgw_zonegroup_list = {}
        rgw_zonegroup_list_cmd = ['zonegroup', 'list']
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_zonegroup_list_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to fetch zonegroup list',
                                         http_status_code=500, component='rgw')
            rgw_zonegroup_list = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return rgw_zonegroup_list

    def get_zonegroup(self, zonegroup_name: str):
        zonegroup_info = {}
        if zonegroup_name != 'default':
            rgw_zonegroup_info_cmd = ['zonegroup', 'get', '--rgw-zonegroup', zonegroup_name]
        else:
            rgw_zonegroup_info_cmd = ['zonegroup', 'get', '--rgw-zonegroup',
                                      zonegroup_name, '--rgw-realm', 'default']
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_zonegroup_info_cmd)
            if exit_code > 0:
                raise DashboardException('Unable to get zonegroup info',
                                         http_status_code=500, component='rgw')
            zonegroup_info = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return zonegroup_info

    def get_all_zonegroups_info(self):
        all_zonegroups_info = {}
        zonegroups_info = []
        rgw_zonegroup_list = self.list_zonegroups()
        if 'zonegroups' in rgw_zonegroup_list:
            if rgw_zonegroup_list['zonegroups'] != []:
                for rgw_zonegroup in rgw_zonegroup_list['zonegroups']:
                    zonegroup_info = self.get_zonegroup(rgw_zonegroup)
                    zonegroups_info.append(zonegroup_info)
                all_zonegroups_info['zonegroups'] = zonegroups_info  # type: ignore
            else:
                all_zonegroups_info['zonegroups'] = []  # type: ignore
        if 'default_info' in rgw_zonegroup_list and rgw_zonegroup_list['default_info'] != '':
            all_zonegroups_info['default_zonegroup'] = rgw_zonegroup_list['default_info']
        else:
            all_zonegroups_info['default_zonegroup'] = ''  # type: ignore
        return all_zonegroups_info

    def delete_zonegroup(self, zonegroup_name: str, delete_pools: str, pools: List[str]):
        if delete_pools == 'true':
            zonegroup_info = self.get_zonegroup(zonegroup_name)
        rgw_delete_zonegroup_cmd = ['zonegroup', 'delete', '--rgw-zonegroup', zonegroup_name]
        try:
            exit_code, _, _ = mgr.send_rgwadmin_command(rgw_delete_zonegroup_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to delete zonegroup',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        self.update_period()
        if delete_pools == 'true':
            for zone in zonegroup_info['zones']:
                self.delete_zone(zone['name'], 'true', pools)

    def modify_zonegroup(self, realm_name: str, zonegroup_name: str, default: str, master: str,
                         endpoints: str):

        rgw_zonegroup_modify_cmd = ['zonegroup', 'modify',
                                    '--rgw-realm', realm_name,
                                    '--rgw-zonegroup', zonegroup_name]
        if endpoints:
            rgw_zonegroup_modify_cmd.append('--endpoints')
            rgw_zonegroup_modify_cmd.append(endpoints)
        if master and str_to_bool(master):
            rgw_zonegroup_modify_cmd.append('--master')
        if default and str_to_bool(default):
            rgw_zonegroup_modify_cmd.append('--default')
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zonegroup_modify_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to modify zonegroup {}'.format(zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        self.update_period()

    def add_or_remove_zone(self, zonegroup_name: str, zone_name: str, action: str):
        if action == 'add':
            rgw_zonegroup_add_zone_cmd = ['zonegroup', 'add', '--rgw-zonegroup',
                                          zonegroup_name, '--rgw-zone', zone_name]
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_zonegroup_add_zone_cmd)
                if exit_code > 0:
                    raise DashboardException(e=err, msg='Unable to add zone {} to zonegroup {}'.format(zone_name, zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
            self.update_period()
        if action == 'remove':
            rgw_zonegroup_rm_zone_cmd = ['zonegroup', 'remove',
                                         '--rgw-zonegroup', zonegroup_name, '--rgw-zone', zone_name]
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_zonegroup_rm_zone_cmd)
                if exit_code > 0:
                    raise DashboardException(e=err, msg='Unable to remove zone {} from zonegroup {}'.format(zone_name, zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
            self.update_period()

    def get_placement_targets_by_zonegroup(self, zonegroup_name: str):
        rgw_get_placement_cmd = ['zonegroup', 'placement',
                                 'list', '--rgw-zonegroup', zonegroup_name]
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(rgw_get_placement_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to get placement targets',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return out

    def add_placement_targets(self, zonegroup_name: str, placement_targets: List[Dict]):
        rgw_add_placement_cmd = ['zonegroup', 'placement', 'add']
        for placement_target in placement_targets:
            cmd_add_placement_options = ['--rgw-zonegroup', zonegroup_name,
                                         '--placement-id', placement_target['placement_id']]
            if placement_target['tags']:
                cmd_add_placement_options += ['--tags', placement_target['tags']]
            rgw_add_placement_cmd += cmd_add_placement_options
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_add_placement_cmd)
                if exit_code > 0:
                    raise DashboardException(e=err,
                                             msg='Unable to add placement target {} to zonegroup {}'.format(placement_target['placement_id'], zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
            self.update_period()
            storage_classes = placement_target['storage_class'].split(",") if placement_target['storage_class'] else []  # noqa E501  #pylint: disable=line-too-long
            if storage_classes:
                for sc in storage_classes:
                    cmd_add_placement_options = ['--storage-class', sc]
                    try:
                        exit_code, _, err = mgr.send_rgwadmin_command(
                            rgw_add_placement_cmd + cmd_add_placement_options)
                        if exit_code > 0:
                            raise DashboardException(e=err,
                                                     msg='Unable to add placement target {} to zonegroup {}'.format(placement_target['placement_id'], zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                                     http_status_code=500, component='rgw')
                    except SubprocessError as error:
                        raise DashboardException(error, http_status_code=500, component='rgw')
                    self.update_period()

    def modify_placement_targets(self, zonegroup_name: str, placement_targets: List[Dict]):
        rgw_add_placement_cmd = ['zonegroup', 'placement', 'modify']
        for placement_target in placement_targets:
            cmd_add_placement_options = ['--rgw-zonegroup', zonegroup_name,
                                         '--placement-id', placement_target['placement_id']]
            if placement_target['tags']:
                cmd_add_placement_options += ['--tags', placement_target['tags']]
            rgw_add_placement_cmd += cmd_add_placement_options
            storage_classes = placement_target['storage_class'].split(",") if placement_target['storage_class'] else []  # noqa E501  #pylint: disable=line-too-long
            if storage_classes:
                for sc in storage_classes:
                    cmd_add_placement_options = []
                    cmd_add_placement_options = ['--storage-class', sc]
                    try:
                        exit_code, _, err = mgr.send_rgwadmin_command(
                            rgw_add_placement_cmd + cmd_add_placement_options)
                        if exit_code > 0:
                            raise DashboardException(e=err,
                                                     msg='Unable to add placement target {} to zonegroup {}'.format(placement_target['placement_id'], zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                                     http_status_code=500, component='rgw')
                    except SubprocessError as error:
                        raise DashboardException(error, http_status_code=500, component='rgw')
                    self.update_period()
            else:
                try:
                    exit_code, _, err = mgr.send_rgwadmin_command(rgw_add_placement_cmd)
                    if exit_code > 0:
                        raise DashboardException(e=err,
                                                 msg='Unable to add placement target {} to zonegroup {}'.format(placement_target['placement_id'], zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                                 http_status_code=500, component='rgw')
                except SubprocessError as error:
                    raise DashboardException(error, http_status_code=500, component='rgw')
                self.update_period()

    # pylint: disable=W0102
    def edit_zonegroup(self, realm_name: str, zonegroup_name: str, new_zonegroup_name: str,
                       default: str = '', master: str = '', endpoints: str = '',
                       add_zones: List[str] = [], remove_zones: List[str] = [],
                       placement_targets: List[Dict[str, str]] = []):
        rgw_zonegroup_edit_cmd = []
        if new_zonegroup_name != zonegroup_name:
            rgw_zonegroup_edit_cmd = ['zonegroup', 'rename', '--rgw-zonegroup', zonegroup_name,
                                      '--zonegroup-new-name', new_zonegroup_name]
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_zonegroup_edit_cmd, False)
                if exit_code > 0:
                    raise DashboardException(e=err, msg='Unable to rename zonegroup to {}'.format(new_zonegroup_name),  # noqa E501  #pylint: disable=line-too-long
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
            self.update_period()
        self.modify_zonegroup(realm_name, new_zonegroup_name, default, master, endpoints)
        if add_zones:
            for zone_name in add_zones:
                self.add_or_remove_zone(new_zonegroup_name, zone_name, 'add')
        if remove_zones:
            for zone_name in remove_zones:
                self.add_or_remove_zone(new_zonegroup_name, zone_name, 'remove')
        existing_placement_targets = self.get_placement_targets_by_zonegroup(new_zonegroup_name)
        existing_placement_targets_ids = [pt['key'] for pt in existing_placement_targets]
        if placement_targets:
            for pt in placement_targets:
                if pt['placement_id'] in existing_placement_targets_ids:
                    self.modify_placement_targets(new_zonegroup_name, placement_targets)
                else:
                    self.add_placement_targets(new_zonegroup_name, placement_targets)

    def update_period(self):
        rgw_update_period_cmd = ['period', 'update', '--commit']
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_update_period_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to update period',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def create_zone(self, zone_name, zonegroup_name, default, master, endpoints, access_key,
                    secret_key):
        rgw_zone_create_cmd = ['zone', 'create']
        cmd_create_zone_options = ['--rgw-zone', zone_name]
        if zonegroup_name != 'null':
            cmd_create_zone_options.append('--rgw-zonegroup')
            cmd_create_zone_options.append(zonegroup_name)
        if default != 'false':
            cmd_create_zone_options.append('--default')
        if master != 'false':
            cmd_create_zone_options.append('--master')
        if endpoints != 'null':
            cmd_create_zone_options.append('--endpoints')
            cmd_create_zone_options.append(endpoints)
        if access_key is not None:
            cmd_create_zone_options.append('--access-key')
            cmd_create_zone_options.append(access_key)
        if secret_key is not None:
            cmd_create_zone_options.append('--secret')
            cmd_create_zone_options.append(secret_key)
        rgw_zone_create_cmd += cmd_create_zone_options
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(rgw_zone_create_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to create zone',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        self.update_period()
        return out

    def parse_secrets(self, user, data):
        for key in data.get('keys', []):
            if key.get('user') == user:
                access_key = key.get('access_key')
                secret_key = key.get('secret_key')
                return access_key, secret_key
        return '', ''

    def modify_zone(self, zone_name: str, zonegroup_name: str, default: str, master: str,
                    endpoints: str, access_key: str, secret_key: str):
        rgw_zone_modify_cmd = ['zone', 'modify', '--rgw-zonegroup',
                               zonegroup_name, '--rgw-zone', zone_name]
        if endpoints:
            rgw_zone_modify_cmd.append('--endpoints')
            rgw_zone_modify_cmd.append(endpoints)
        if default and str_to_bool(default):
            rgw_zone_modify_cmd.append('--default')
        if master and str_to_bool(master):
            rgw_zone_modify_cmd.append('--master')
        if access_key is not None:
            rgw_zone_modify_cmd.append('--access-key')
            rgw_zone_modify_cmd.append(access_key)
        if secret_key is not None:
            rgw_zone_modify_cmd.append('--secret')
            rgw_zone_modify_cmd.append(secret_key)
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_modify_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to modify zone',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        self.update_period()

    def add_placement_targets_zone(self, zone_name: str, placement_target: str, data_pool: str,
                                   index_pool: str, data_extra_pool: str):
        rgw_zone_add_placement_cmd = ['zone', 'placement', 'add', '--rgw-zone', zone_name,
                                      '--placement-id', placement_target, '--data-pool', data_pool,
                                      '--index-pool', index_pool,
                                      '--data-extra-pool', data_extra_pool]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_add_placement_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to add placement target {} to zone {}'.format(placement_target, zone_name),  # noqa E501 #pylint: disable=line-too-long
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        self.update_period()

    def add_storage_class_zone(self, zone_name: str, placement_target: str, storage_class: str,
                               data_pool: str, compression: str):
        rgw_zone_add_storage_class_cmd = ['zone', 'placement', 'add', '--rgw-zone', zone_name,
                                          '--placement-id', placement_target,
                                          '--storage-class', storage_class,
                                          '--data-pool', data_pool,
                                          '--compression', compression]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_add_storage_class_cmd)
            if exit_code > 0:
                raise DashboardException(e=err, msg='Unable to add storage class {} to zone {}'.format(storage_class, zone_name),  # noqa E501 #pylint: disable=line-too-long
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        self.update_period()

    def edit_zone(self, zone_name: str, new_zone_name: str, zonegroup_name: str, default: str = '',
                  master: str = '', endpoints: str = '', access_key: str = '', secret_key: str = '',
                  placement_target: str = '', data_pool: str = '', index_pool: str = '',
                  data_extra_pool: str = '', storage_class: str = '', data_pool_class: str = '',
                  compression: str = ''):
        if new_zone_name != zone_name:
            rgw_zone_rename_cmd = ['zone', 'rename', '--rgw-zone',
                                   zone_name, '--zone-new-name', new_zone_name]
            try:
                exit_code, _, err = mgr.send_rgwadmin_command(rgw_zone_rename_cmd, False)
                if exit_code > 0:
                    raise DashboardException(e=err, msg='Unable to rename zone to {}'.format(new_zone_name),  # noqa E501 #pylint: disable=line-too-long
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
            self.update_period()
        self.modify_zone(new_zone_name, zonegroup_name, default, master, endpoints, access_key,
                         secret_key)
        self.add_placement_targets_zone(new_zone_name, placement_target,
                                        data_pool, index_pool, data_extra_pool)
        self.add_storage_class_zone(new_zone_name, placement_target, storage_class,
                                    data_pool_class, compression)

    def list_zones(self):
        rgw_zone_list = {}
        rgw_zone_list_cmd = ['zone', 'list']
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_zone_list_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to fetch zone list',
                                         http_status_code=500, component='rgw')
            rgw_zone_list = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return rgw_zone_list

    def get_zone(self, zone_name: str):
        zone_info = {}
        rgw_zone_info_cmd = ['zone', 'get', '--rgw-zone', zone_name]
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_zone_info_cmd)
            if exit_code > 0:
                raise DashboardException('Unable to get zone info',
                                         http_status_code=500, component='rgw')
            zone_info = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return zone_info

    def get_all_zones_info(self):
        all_zones_info = {}
        zones_info = []
        rgw_zone_list = self.list_zones()
        if 'zones' in rgw_zone_list:
            if rgw_zone_list['zones'] != []:
                for rgw_zone in rgw_zone_list['zones']:
                    zone_info = self.get_zone(rgw_zone)
                    zones_info.append(zone_info)
                    all_zones_info['zones'] = zones_info  # type: ignore
            else:
                all_zones_info['zones'] = []
        if 'default_info' in rgw_zone_list and rgw_zone_list['default_info'] != '':
            all_zones_info['default_zone'] = rgw_zone_list['default_info']  # type: ignore
        else:
            all_zones_info['default_zone'] = ''  # type: ignore
        return all_zones_info

    def delete_zone(self, zone_name: str, delete_pools: str, pools: List[str],
                    zonegroup_name: str = '',):
        rgw_remove_zone_from_zonegroup_cmd = ['zonegroup', 'remove', '--rgw-zonegroup',
                                              zonegroup_name, '--rgw-zone', zone_name]
        rgw_delete_zone_cmd = ['zone', 'delete', '--rgw-zone', zone_name]
        if zonegroup_name:
            try:
                exit_code, _, _ = mgr.send_rgwadmin_command(rgw_remove_zone_from_zonegroup_cmd)
                if exit_code > 0:
                    raise DashboardException(msg='Unable to remove zone from zonegroup',
                                             http_status_code=500, component='rgw')
            except SubprocessError as error:
                raise DashboardException(error, http_status_code=500, component='rgw')
            self.update_period()
        try:
            exit_code, _, _ = mgr.send_rgwadmin_command(rgw_delete_zone_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to delete zone',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        self.update_period()
        if delete_pools == 'true':
            self.delete_pools(pools)

    def delete_pools(self, pools):
        for pool in pools:
            if mgr.rados.pool_exists(pool):
                mgr.rados.delete_pool(pool)

    def create_system_user(self, userName: str, zoneName: str):
        rgw_user_create_cmd = ['user', 'create', '--uid', userName,
                               '--display-name', userName, '--rgw-zone', zoneName, '--system']
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_user_create_cmd)
            if exit_code > 0:
                raise DashboardException(msg='Unable to create system user',
                                         http_status_code=500, component='rgw')
            return out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def get_user_list(self, zoneName: str):
        all_users_info = []
        user_list = []
        rgw_user_list_cmd = ['user', 'list', '--rgw-zone', zoneName]
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_user_list_cmd)
            if exit_code > 0:
                raise DashboardException('Unable to get user list',
                                         http_status_code=500, component='rgw')
            user_list = out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

        if len(user_list) > 0:
            for user_name in user_list:
                rgw_user_info_cmd = ['user', 'info', '--uid', user_name, '--rgw-zone', zoneName]
                try:
                    exit_code, out, _ = mgr.send_rgwadmin_command(rgw_user_info_cmd)
                    if exit_code > 0:
                        raise DashboardException('Unable to get user info',
                                                 http_status_code=500, component='rgw')
                    all_users_info.append(out)
                except SubprocessError as error:
                    raise DashboardException(error, http_status_code=500, component='rgw')
        return all_users_info

    def get_multisite_status(self):
        is_multisite_configured = True
        rgw_realm_list = self.list_realms()
        rgw_zonegroup_list = self.list_zonegroups()
        rgw_zone_list = self.list_zones()
        if len(rgw_realm_list['realms']) < 1 and len(rgw_zonegroup_list['zonegroups']) <= 1 \
                and len(rgw_zone_list['zones']) <= 1:
            is_multisite_configured = False
        return is_multisite_configured

    def get_multisite_sync_status(self):
        rgw_multisite_sync_status_cmd = ['sync', 'status']
        try:
            exit_code, out, _ = mgr.send_rgwadmin_command(rgw_multisite_sync_status_cmd, False)
            if exit_code > 0:
                raise DashboardException('Unable to get sync status',
                                         http_status_code=500, component='rgw')
            if out:
                return self.process_data(out)
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')
        return {}

    def process_data(self, data):
        primary_zone_data, metadata_sync_data = self.extract_metadata_and_primary_zone_data(data)
        replica_zones_info = []
        if metadata_sync_data != {}:
            datasync_info = self.extract_datasync_info(data)
            replica_zones_info = [self.extract_replica_zone_data(item) for item in datasync_info]

        replica_zones_info_object = {
            'metadataSyncInfo': metadata_sync_data,
            'dataSyncInfo': replica_zones_info,
            'primaryZoneData': primary_zone_data
        }

        return replica_zones_info_object

    def extract_metadata_and_primary_zone_data(self, data):
        primary_zone_info, metadata_sync_infoormation = self.extract_zones_data(data)

        primary_zone_tree = primary_zone_info.split('\n') if primary_zone_info else []
        realm = self.get_primary_zonedata(primary_zone_tree[0])
        zonegroup = self.get_primary_zonedata(primary_zone_tree[1])
        zone = self.get_primary_zonedata(primary_zone_tree[2])

        primary_zone_data = [realm, zonegroup, zone]
        zonegroup_info = self.get_zonegroup(zonegroup)
        metadata_sync_data = {}
        if len(zonegroup_info['zones']) > 1:
            metadata_sync_data = self.extract_metadata_sync_data(metadata_sync_infoormation)

        return primary_zone_data, metadata_sync_data

    def extract_zones_data(self, data):
        result = data
        primary_zone_info = result.split('metadata sync')[0] if 'metadata sync' in result else None
        metadata_sync_infoormation = result.split('metadata sync')[1] if 'metadata sync' in result else None  # noqa E501  #pylint: disable=line-too-long
        return primary_zone_info, metadata_sync_infoormation

    def extract_metadata_sync_data(self, metadata_sync_infoormation):
        metadata_sync_info = metadata_sync_infoormation.split('data sync source')[0].strip() if 'data sync source' in metadata_sync_infoormation else None  # noqa E501  #pylint: disable=line-too-long

        if metadata_sync_info == 'no sync (zone is master)':
            return metadata_sync_info

        metadata_sync_data = {}
        metadata_sync_info_array = metadata_sync_info.split('\n') if metadata_sync_info else []
        metadata_sync_data['syncstatus'] = metadata_sync_info_array[0].strip() if len(metadata_sync_info_array) > 0 else None  # noqa E501  #pylint: disable=line-too-long

        for item in metadata_sync_info_array:
            self.extract_metadata_sync_info(metadata_sync_data, item)

        metadata_sync_data['fullSyncStatus'] = metadata_sync_info_array
        return metadata_sync_data

    def extract_metadata_sync_info(self, metadata_sync_data, item):
        if 'oldest incremental change not applied:' in item:
            metadata_sync_data['timestamp'] = item.split('applied:')[1].split()[0].strip()

    def extract_datasync_info(self, data):
        metadata_sync_infoormation = data.split('metadata sync')[1] if 'metadata sync' in data else None  # noqa E501  #pylint: disable=line-too-long
        if 'data sync source' in metadata_sync_infoormation:
            datasync_info = metadata_sync_infoormation.split('data sync source')[1].split('source:')
            return datasync_info
        return []

    def extract_replica_zone_data(self, datasync_item):
        replica_zone_data = {}
        datasync_info_array = datasync_item.split('\n')
        replica_zone_name = self.get_primary_zonedata(datasync_info_array[0])
        replica_zone_data['name'] = replica_zone_name.strip()
        replica_zone_data['syncstatus'] = datasync_info_array[1].strip()
        replica_zone_data['fullSyncStatus'] = datasync_info_array
        for item in datasync_info_array:
            self.extract_metadata_sync_info(replica_zone_data, item)
        return replica_zone_data

    def get_primary_zonedata(self, data):
        regex = r'\(([^)]+)\)'
        match = re.search(regex, data)

        if match and match.group(1):
            return match.group(1)

        return ''

    def get_sync_policy(self, bucket_name: str = '', zonegroup_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'policy', 'get']
        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]
        if zonegroup_name:
            rgw_sync_policy_cmd += ['--rgw-zonegroup', zonegroup_name]
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to get sync policy: {err}',
                                         http_status_code=500, component='rgw')
            return out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def get_sync_policy_group(self, group_id: str, bucket_name: str = '',
                              zonegroup_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'get', '--group-id', group_id]
        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]
        if zonegroup_name:
            rgw_sync_policy_cmd += ['--rgw-zonegroup', zonegroup_name]
        try:
            exit_code, out, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to get sync policy group: {err}',
                                         http_status_code=500, component='rgw')
            return out
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def create_sync_policy_group(self, group_id: str, status: str, bucket_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'create', '--group-id', group_id,
                               '--status', SyncStatus[status].value]
        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to create sync policy group: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def update_sync_policy_group(self, group_id: str, status: str, bucket_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'modify', '--group-id', group_id,
                               '--status', SyncStatus[status].value]
        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to update sync policy group: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def remove_sync_policy_group(self, group_id: str, bucket_name=''):
        rgw_sync_policy_cmd = ['sync', 'group', 'remove', '--group-id', group_id]
        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]
        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to remove sync policy group: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def create_sync_flow(self, group_id: str, flow_id: str, flow_type: str,
                         zones: Optional[List[str]] = None, bucket_name: str = '',
                         source_zone: str = '', destination_zone: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'flow', 'create', '--group-id', group_id,
                               '--flow-id', flow_id, '--flow-type', SyncFlowTypes[flow_type].value]

        if SyncFlowTypes[flow_type].value == 'directional':
            rgw_sync_policy_cmd += ['--source-zone', source_zone, '--dest-zone', destination_zone]
        else:
            if zones:
                rgw_sync_policy_cmd += ['--zones', ','.join(zones)]

        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]

        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to create sync flow: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def remove_sync_flow(self, group_id: str, flow_id: str, flow_type: str,
                         source_zone='', destination_zone='',
                         zones: Optional[List[str]] = None, bucket_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'flow', 'remove', '--group-id', group_id,
                               '--flow-id', flow_id, '--flow-type', SyncFlowTypes[flow_type].value]

        if SyncFlowTypes[flow_type].value == 'directional':
            rgw_sync_policy_cmd += ['--source-zone', source_zone, '--dest-zone', destination_zone]
        else:
            if zones:
                rgw_sync_policy_cmd += ['--zones', ','.join(zones)]

        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]

        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to remove sync flow: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def create_sync_pipe(self, group_id: str, pipe_id: str,
                         source_zones: Optional[List[str]] = None,
                         destination_zones: Optional[List[str]] = None,
                         destination_buckets: Optional[List[str]] = None, bucket_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'pipe', 'create',
                               '--group-id', group_id, '--pipe-id', pipe_id]

        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]

        if source_zones:
            rgw_sync_policy_cmd += ['--source-zones', ','.join(source_zones)]

        if destination_zones:
            rgw_sync_policy_cmd += ['--dest-zones', ','.join(destination_zones)]

        if destination_buckets:
            rgw_sync_policy_cmd += ['--dest-bucket', ','.join(destination_buckets)]

        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to create sync pipe: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def remove_sync_pipe(self, group_id: str, pipe_id: str,
                         source_zones: Optional[List[str]] = None,
                         destination_zones: Optional[List[str]] = None,
                         destination_buckets: Optional[List[str]] = None, bucket_name: str = ''):
        rgw_sync_policy_cmd = ['sync', 'group', 'pipe', 'remove',
                               '--group-id', group_id, '--pipe-id', pipe_id]

        if bucket_name:
            rgw_sync_policy_cmd += ['--bucket', bucket_name]

        if source_zones:
            rgw_sync_policy_cmd += ['--source-zones', ','.join(source_zones)]

        if destination_zones:
            rgw_sync_policy_cmd += ['--dest-zones', ','.join(destination_zones)]

        if destination_buckets:
            rgw_sync_policy_cmd += ['--dest-bucket', ','.join(destination_buckets)]

        try:
            exit_code, _, err = mgr.send_rgwadmin_command(rgw_sync_policy_cmd)
            if exit_code > 0:
                raise DashboardException(f'Unable to remove sync pipe: {err}',
                                         http_status_code=500, component='rgw')
        except SubprocessError as error:
            raise DashboardException(error, http_status_code=500, component='rgw')

    def create_dashboard_admin_sync_group(self, zonegroup_name: str = ''):

        zonegroup_info = self.get_zonegroup(zonegroup_name)
        zone_names = []
        for zones in zonegroup_info['zones']:
            zone_names.append(zones['name'])

        # create a sync policy group with status allowed
        self.create_sync_policy_group(_SYNC_GROUP_ID, SyncStatus.allowed.value)
        # create a sync flow with source and destination zones
        self.create_sync_flow(_SYNC_GROUP_ID, _SYNC_FLOW_ID,
                              SyncFlowTypes.symmetrical.value,
                              zones=zone_names)
        # create a sync pipe with source and destination zones
        self.create_sync_pipe(_SYNC_GROUP_ID, _SYNC_PIPE_ID, source_zones=['*'],
                              destination_zones=['*'], destination_buckets=['*'])
        # period update --commit
        self.update_period()

    def policy_group_exists(self, group_name: str, zonegroup_name: str):
        try:
            _ = self.get_sync_policy_group(
                group_id=group_name, zonegroup_name=zonegroup_name)
            return True
        except DashboardException:
            return False
