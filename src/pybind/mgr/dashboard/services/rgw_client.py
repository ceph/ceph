# -*- coding: utf-8 -*-

import ipaddress
import logging
import re
import xml.etree.ElementTree as ET  # noqa: N814
from distutils.util import strtobool

from .. import mgr
from ..awsauth import S3Auth
from ..exceptions import DashboardException
from ..rest_client import RequestException, RestClient
from ..settings import Settings
from ..tools import build_url, dict_contains_path, dict_get, json_str_to_object

try:
    from typing import Any, Dict, List, Optional, Tuple, Union
except ImportError:
    pass  # For typing only

logger = logging.getLogger('rgw_client')


class NoRgwDaemonsException(Exception):
    def __init__(self):
        super().__init__('No RGW service is running.')


class NoCredentialsException(RequestException):
    def __init__(self):
        super(NoCredentialsException, self).__init__(
            'No RGW credentials found, '
            'please consult the documentation on how to enable RGW for '
            'the dashboard.')


class RgwDaemon:
    """Simple representation of a daemon."""
    host: str
    name: str
    port: int
    ssl: bool
    zonegroup_name: str


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
            daemon.zonegroup_name = daemon_map[key]['metadata']['zonegroup_name']
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
    daemon.host = _parse_addr(daemon_info['addr'])
    daemon.port, daemon.ssl = _parse_frontend_config(daemon_info['metadata']['frontend_config#0'])

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
            access_key = Settings.RGW_API_ACCESS_KEY[daemon_name]
            secret_key = Settings.RGW_API_SECRET_KEY[daemon_name]
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

    @staticmethod
    def _rgw_settings():
        return (Settings.RGW_API_HOST,
                Settings.RGW_API_PORT,
                Settings.RGW_API_ACCESS_KEY,
                Settings.RGW_API_SECRET_KEY,
                Settings.RGW_API_ADMIN_RESOURCE,
                Settings.RGW_API_SCHEME,
                Settings.RGW_API_SSL_VERIFY)

    @staticmethod
    def instance(userid: Optional[str] = None,
                 daemon_name: Optional[str] = None) -> 'RgwClient':
        # pylint: disable=too-many-branches

        RgwClient._daemons = _get_daemons()

        # The API access key and secret key are mandatory for a minimal configuration.
        if not (Settings.RGW_API_ACCESS_KEY and Settings.RGW_API_SECRET_KEY):
            logger.warning('No credentials found, please consult the '
                           'documentation about how to enable RGW for the '
                           'dashboard.')
            raise NoCredentialsException()

        if not daemon_name:
            # Select default daemon if configured in settings:
            if Settings.RGW_API_HOST and Settings.RGW_API_PORT:
                for daemon in RgwClient._daemons.values():
                    if daemon.host == Settings.RGW_API_HOST \
                            and daemon.port == Settings.RGW_API_PORT:
                        daemon_name = daemon.name
                        break
                if not daemon_name:
                    raise DashboardException(
                        msg='No RGW daemon found with user-defined host: {}, port: {}'.format(
                            Settings.RGW_API_HOST,
                            Settings.RGW_API_PORT),
                        http_status_code=404,
                        component='rgw')
            # Select 1st daemon:
            else:
                daemon_name = next(iter(RgwClient._daemons.keys()))

        # Discard all cached instances if any rgw setting has changed
        if RgwClient._rgw_settings_snapshot != RgwClient._rgw_settings():
            RgwClient._rgw_settings_snapshot = RgwClient._rgw_settings()
            RgwClient.drop_instance()

        if daemon_name not in RgwClient._config_instances:
            connection_info = RgwClient._get_daemon_connection_info(daemon_name)
            RgwClient._config_instances[daemon_name] = RgwClient(connection_info['access_key'],
                                                                 connection_info['secret_key'],
                                                                 daemon_name)

        if not userid or userid == RgwClient._config_instances[daemon_name].userid:
            return RgwClient._config_instances[daemon_name]

        if daemon_name not in RgwClient._user_instances \
                or userid not in RgwClient._user_instances[daemon_name]:
            # Get the access and secret keys for the specified user.
            keys = RgwClient._config_instances[daemon_name].get_user_keys(userid)
            if not keys:
                raise RequestException(
                    "User '{}' does not have any keys configured.".format(
                        userid))
            instance = RgwClient(keys['access_key'],
                                 keys['secret_key'],
                                 daemon_name,
                                 userid)
            RgwClient._user_instances.update({daemon_name: {userid: instance}})

        return RgwClient._user_instances[daemon_name][userid]

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
                 access_key,
                 secret_key,
                 daemon_name,
                 user_id=None):
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
        return strtobool(response['data']['system'])

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

        # Do some validations.
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
