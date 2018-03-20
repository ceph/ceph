# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
from ..awsauth import S3Auth
from ..settings import Settings, Options
from ..rest_client import RestClient, RequestException
from ..tools import build_url, dict_contains_path
from ..exceptions import NoCredentialsException
from .. import mgr, logger


def _determine_rgw_addr():
    service_map = mgr.get('service_map')

    if not dict_contains_path(service_map, ['services', 'rgw', 'daemons', 'rgw']):
        msg = 'No RGW found.'
        raise LookupError(msg)

    daemon = service_map['services']['rgw']['daemons']['rgw']
    addr = daemon['addr'].split(':')[0]
    match = re.search(r'port=(\d+)',
                      daemon['metadata']['frontend_config#0'])
    if match:
        port = int(match.group(1))
    else:
        msg = 'Failed to determine RGW port'
        raise LookupError(msg)

    return addr, port


class RgwClient(RestClient):
    _SYSTEM_USERID = None
    _ADMIN_PATH = None
    _host = None
    _port = None
    _ssl = None
    _user_instances = {}

    @staticmethod
    def _load_settings():
        if Settings.RGW_API_SCHEME and Settings.RGW_API_ACCESS_KEY and \
                Settings.RGW_API_SECRET_KEY:
            if Options.has_default_value('RGW_API_HOST') and \
                    Options.has_default_value('RGW_API_PORT'):
                host, port = _determine_rgw_addr()
            else:
                host, port = Settings.RGW_API_HOST, Settings.RGW_API_PORT
        else:
            logger.warning('No credentials found, please consult the '
                           'documentation about how to enable RGW for the '
                           'dashboard.')
            raise NoCredentialsException()

        RgwClient._host = host
        RgwClient._port = port
        RgwClient._ssl = Settings.RGW_API_SCHEME == 'https'
        RgwClient._ADMIN_PATH = Settings.RGW_API_ADMIN_RESOURCE
        RgwClient._SYSTEM_USERID = Settings.RGW_API_USER_ID

        logger.info("Creating new connection for user: %s",
                    Settings.RGW_API_USER_ID)
        RgwClient._user_instances[RgwClient._SYSTEM_USERID] = \
            RgwClient(Settings.RGW_API_USER_ID, Settings.RGW_API_ACCESS_KEY,
                      Settings.RGW_API_SECRET_KEY)

    @staticmethod
    def instance(userid):
        if not RgwClient._user_instances:
            RgwClient._load_settings()
        if not userid:
            userid = RgwClient._SYSTEM_USERID
        if userid not in RgwClient._user_instances:
            logger.info("Creating new connection for user: %s", userid)
            keys = RgwClient.admin_instance().get_user_keys(userid)
            if not keys:
                raise Exception(
                    "User '{}' does not have any keys configured.".format(
                        userid))

            RgwClient._user_instances[userid] = RgwClient(
                userid, keys['access_key'], keys['secret_key'])
        return RgwClient._user_instances[userid]

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
                 admin_path='admin',
                 ssl=False):

        if not host and not RgwClient._host:
            RgwClient._load_settings()
        host = host if host else RgwClient._host
        port = port if port else RgwClient._port
        admin_path = admin_path if admin_path else RgwClient._ADMIN_PATH
        ssl = ssl if ssl else RgwClient._ssl

        self.userid = userid
        self.service_url = build_url(host=host, port=port)
        self.admin_path = admin_path

        s3auth = S3Auth(access_key, secret_key, service_url=self.service_url)
        super(RgwClient, self).__init__(host, port, 'RGW', ssl, s3auth)

        logger.info("Creating new connection")

    @RestClient.api_get('/', resp_structure='[0] > ID')
    def is_service_online(self, request=None):
        response = request({'format': 'json'})
        return response[0]['ID'] == 'online'

    @RestClient.api_get('/{admin_path}/metadata/user', resp_structure='[+]')
    def _is_system_user(self, admin_path, request=None):
        # pylint: disable=unused-argument
        response = request()
        return self.userid in response

    def is_system_user(self):
        return self._is_system_user(self.admin_path)

    @RestClient.api_get(
        '/{admin_path}/user',
        resp_structure='tenant & user_id & email & keys[*] > '
        ' (user & access_key & secret_key)')
    def _admin_get_user_keys(self, admin_path, userid, request=None):
        # pylint: disable=unused-argument
        colon_idx = userid.find(':')
        user = userid if colon_idx == -1 else userid[:colon_idx]
        response = request({'uid': user})
        for keys in response['keys']:
            if keys['user'] == userid:
                return {
                    'access_key': keys['access_key'],
                    'secret_key': keys['secret_key']
                }
        return None

    def get_user_keys(self, userid):
        return self._admin_get_user_keys(self.admin_path, userid)

    @RestClient.api('/{admin_path}/{path}')
    def _proxy_request(self,  # pylint: disable=too-many-arguments
                       admin_path,
                       path,
                       method,
                       params,
                       data,
                       request=None):
        # pylint: disable=unused-argument
        return request(
            method=method, params=params, data=data, raw_content=True)

    def proxy(self, method, path, params, data):
        logger.debug("proxying method=%s path=%s params=%s data=%s", method,
                     path, params, data)
        return self._proxy_request(self.admin_path, path, method, params, data)

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
            else:
                raise e

    @RestClient.api_put('/{bucket_name}')
    def create_bucket(self, bucket_name, request=None):
        logger.info("Creating bucket: %s", bucket_name)
        return request()
