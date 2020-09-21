# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import logging
import ipaddress
from distutils.util import strtobool
import xml.etree.ElementTree as ET  # noqa: N814
from ..awsauth import S3Auth
from ..exceptions import DashboardException
from ..settings import Settings, Options
from ..rest_client import RestClient, RequestException
from ..tools import build_url, dict_contains_path, json_str_to_object,\
                    partial_dict, dict_get
from .. import mgr

try:
    from typing import Dict, List, Optional  # pylint: disable=unused-import
except ImportError:
    pass  # For typing only

logger = logging.getLogger('rgw_client')


class NoCredentialsException(RequestException):
    def __init__(self):
        super(NoCredentialsException, self).__init__(
            'No RGW credentials found, '
            'please consult the documentation on how to enable RGW for '
            'the dashboard.')


def _determine_rgw_addr():
    """
    Get a RGW daemon to determine the configured host (IP address) and port.
    Note, the service id of the RGW daemons may differ depending on the setup.
    Example 1:
    {
        ...
        'services': {
            'rgw': {
                'daemons': {
                    'summary': '',
                    '0': {
                        ...
                        'addr': '[2001:db8:85a3::8a2e:370:7334]:49774/1534999298',
                        'metadata': {
                            'frontend_config#0': 'civetweb port=7280',
                        }
                        ...
                    }
                }
            }
        }
    }
    Example 2:
    {
        ...
        'services': {
            'rgw': {
                'daemons': {
                    'summary': '',
                    'rgw': {
                        ...
                        'addr': '192.168.178.3:49774/1534999298',
                        'metadata': {
                            'frontend_config#0': 'civetweb port=8000',
                        }
                        ...
                    }
                }
            }
        }
    }
    """
    service_map = mgr.get('service_map')
    if not dict_contains_path(service_map, ['services', 'rgw', 'daemons']):
        raise LookupError('No RGW found')
    daemon = None
    daemons = service_map['services']['rgw']['daemons']
    for key in daemons.keys():
        if dict_contains_path(daemons[key], ['metadata', 'frontend_config#0']):
            daemon = daemons[key]
            break
    if daemon is None:
        raise LookupError('No RGW daemon found')

    addr = _parse_addr(daemon['addr'])
    port, ssl = _parse_frontend_config(daemon['metadata']['frontend_config#0'])

    return addr, port, ssl


def _parse_addr(value):
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


def _parse_frontend_config(config):
    """
    Get the port the RGW is running on. Due the complexity of the
    syntax not all variations are supported.

    If there are multiple (ssl_)ports/(ssl_)endpoints options, then
    the first found option will be returned.

    Get more details about the configuration syntax here:
    http://docs.ceph.com/docs/master/radosgw/frontends/
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
    _SYSTEM_USERID = None
    _ADMIN_PATH = None
    _host = None
    _port = None
    _ssl = None
    _user_instances = {}  # type: Dict[str, RgwClient]
    _rgw_settings_snapshot = None

    @staticmethod
    def _load_settings():
        # The API access key and secret key are mandatory for a minimal configuration.
        if not (Settings.RGW_API_ACCESS_KEY and Settings.RGW_API_SECRET_KEY):
            logger.warning('No credentials found, please consult the '
                           'documentation about how to enable RGW for the '
                           'dashboard.')
            raise NoCredentialsException()

        if Options.has_default_value('RGW_API_HOST') and \
                Options.has_default_value('RGW_API_PORT') and \
                Options.has_default_value('RGW_API_SCHEME'):
            host, port, ssl = _determine_rgw_addr()
        else:
            host = Settings.RGW_API_HOST
            port = Settings.RGW_API_PORT
            ssl = Settings.RGW_API_SCHEME == 'https'

        RgwClient._host = host
        RgwClient._port = port
        RgwClient._ssl = ssl
        RgwClient._ADMIN_PATH = Settings.RGW_API_ADMIN_RESOURCE

        # Create an instance using the configured settings.
        instance = RgwClient(Settings.RGW_API_USER_ID,
                             Settings.RGW_API_ACCESS_KEY,
                             Settings.RGW_API_SECRET_KEY)

        RgwClient._SYSTEM_USERID = instance.userid

        # Append the instance to the internal map.
        RgwClient._user_instances[RgwClient._SYSTEM_USERID] = instance

    def _get_daemon_zone_info(self):  # type: () -> dict
        return json_str_to_object(self.proxy('GET', 'config?type=zone', None, None))

    def _get_daemon_zonegroup_map(self):  # type: () -> List[dict]
        zonegroups = json_str_to_object(
            self.proxy('GET', 'config?type=zonegroup-map', None, None)
        )

        return [partial_dict(
            zonegroup['val'],
            ['api_name', 'zones']
        ) for zonegroup in zonegroups['zonegroups']]

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
                Settings.RGW_API_USER_ID,
                Settings.RGW_API_SSL_VERIFY)

    @staticmethod
    def instance(userid):
        # type: (Optional[str]) -> RgwClient
        # Discard all cached instances if any rgw setting has changed
        if RgwClient._rgw_settings_snapshot != RgwClient._rgw_settings():
            RgwClient._rgw_settings_snapshot = RgwClient._rgw_settings()
            RgwClient._user_instances.clear()

        if not RgwClient._user_instances:
            RgwClient._load_settings()

        if not userid:
            userid = RgwClient._SYSTEM_USERID

        if userid not in RgwClient._user_instances:
            # Get the access and secret keys for the specified user.
            keys = RgwClient.admin_instance().get_user_keys(userid)
            if not keys:
                raise RequestException(
                    "User '{}' does not have any keys configured.".format(
                        userid))

            # Create an instance and append it to the internal map.
            RgwClient._user_instances[userid] = RgwClient(userid,  # type: ignore
                                                          keys['access_key'],
                                                          keys['secret_key'])

        return RgwClient._user_instances[userid]  # type: ignore

    @staticmethod
    def admin_instance():
        return RgwClient.instance(RgwClient._SYSTEM_USERID)

    def _reset_login(self):
        if self.userid != RgwClient._SYSTEM_USERID:
            logger.info("Fetching new keys for user: %s", self.userid)
            keys = RgwClient.admin_instance().get_user_keys(self.userid)
            self.auth = S3Auth(keys['access_key'], keys['secret_key'],
                               service_url=self.service_url)
        else:
            raise RequestException('Authentication failed for the "{}" user: wrong credentials'
                                   .format(self.userid), status_code=401)

    def __init__(self,  # pylint: disable-msg=R0913
                 userid,
                 access_key,
                 secret_key,
                 host=None,
                 port=None,
                 admin_path=None,
                 ssl=False):

        if not host and not RgwClient._host:
            RgwClient._load_settings()
        host = host if host else RgwClient._host
        port = port if port else RgwClient._port
        admin_path = admin_path if admin_path else RgwClient._ADMIN_PATH
        ssl = ssl if ssl else RgwClient._ssl
        ssl_verify = Settings.RGW_API_SSL_VERIFY

        self.service_url = build_url(host=host, port=port)
        self.admin_path = admin_path

        s3auth = S3Auth(access_key, secret_key, service_url=self.service_url)
        super(RgwClient, self).__init__(host, port, 'RGW', ssl, s3auth, ssl_verify=ssl_verify)

        # If user ID is not set, then try to get it via the RGW Admin Ops API.
        self.userid = userid if userid else self._get_user_id(self.admin_path)  # type: str

        logger.info("Created new connection: user=%s, host=%s, port=%s, ssl=%d, sslverify=%d",
                    self.userid, host, port, ssl, ssl_verify)

    @RestClient.api_get('/', resp_structure='[0] > (ID & DisplayName)')
    def is_service_online(self, request=None):
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
    def _is_system_user(self, admin_path, userid, request=None):
        # pylint: disable=unused-argument
        response = request()
        return strtobool(response['data']['system'])

    def is_system_user(self):
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
        # A zone without realm id can only belong to default zonegroup.
        zonegroup_name = 'default'
        if zone['realm_id']:
            zonegroup_map = self._get_daemon_zonegroup_map()
            for zonegroup in zonegroup_map:
                for realm_zone in zonegroup['zones']:
                    if realm_zone['id'] == zone['id']:
                        zonegroup_name = zonegroup['api_name']
                        break

        placement_targets = []  # type: List[Dict]
        for placement_pool in zone['placement_pools']:
            placement_targets.append(
                {
                    'name': placement_pool['key'],
                    'data_pool': placement_pool['val']['storage_classes']['STANDARD']['data_pool']
                }
            )

        return {'zonegroup': zonegroup_name, 'placement_targets': placement_targets}

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
            if error.status_code == 403:
                msg = 'Bad MFA credentials: {}'.format(msg)
            # Avoid dashboard GUI redirections caused by status code (403, ...):
            http_status_code = 400 if 400 <= error.status_code < 500 else error.status_code
            raise DashboardException(msg=msg,
                                     http_status_code=http_status_code,
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
                           bucket_name,
                           mode,
                           retention_period_days,
                           retention_period_years,
                           request=None):
        # type: (str, str, int, int, Optional[object]) -> None
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
        if retention_period_days and retention_period_years:
            # https://docs.aws.amazon.com/AmazonS3/latest/API/archive-RESTBucketPUTObjectLockConfiguration.html
            msg = "Retention period requires either Days or Years. "\
                "You can't specify both at the same time."
            raise DashboardException(msg=msg, component='rgw')
        if not retention_period_days and not retention_period_years:
            msg = "Retention period requires either Days or Years. "\
                "You must specify at least one."
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
