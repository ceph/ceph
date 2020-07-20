# -*- coding: utf-8 -*-
# pylint: disable=W0212,too-many-return-statements
from __future__ import absolute_import

import json
import logging
from collections import namedtuple
import time

import requests
import six
from teuthology.exceptions import CommandFailedError

from tasks.mgr.mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class DashboardTestCase(MgrTestCase):
    # Display full error diffs
    maxDiff = None

    # Increased x3 (20 -> 60)
    TIMEOUT_HEALTH_CLEAR = 60

    MGRS_REQUIRED = 2
    MDSS_REQUIRED = 1
    REQUIRE_FILESYSTEM = True
    CLIENTS_REQUIRED = 1
    CEPHFS = False

    _session = None  # type: requests.sessions.Session
    _token = None
    _resp = None  # type: requests.models.Response
    _loggedin = False
    _base_uri = None

    AUTO_AUTHENTICATE = True

    AUTH_ROLES = ['administrator']

    @classmethod
    def create_user(cls, username, password, roles=None,
                    force_password=True, cmd_args=None):
        """
        :param username: The name of the user.
        :type username: str
        :param password: The password.
        :type password: str
        :param roles: A list of roles.
        :type roles: list
        :param force_password: Force the use of the specified password. This
          will bypass the password complexity check. Defaults to 'True'.
        :type force_password: bool
        :param cmd_args: Additional command line arguments for the
          'ac-user-create' command.
        :type cmd_args: None | list[str]
        """
        try:
            cls._ceph_cmd(['dashboard', 'ac-user-show', username])
            cls._ceph_cmd(['dashboard', 'ac-user-delete', username])
        except CommandFailedError as ex:
            if ex.exitstatus != 2:
                raise ex

        user_create_args = [
            'dashboard', 'ac-user-create', username, password
        ]
        if force_password:
            user_create_args.append('--force-password')
        if cmd_args:
            user_create_args.extend(cmd_args)
        cls._ceph_cmd(user_create_args)

        if roles:
            set_roles_args = ['dashboard', 'ac-user-set-roles', username]
            for idx, role in enumerate(roles):
                if isinstance(role, str):
                    set_roles_args.append(role)
                else:
                    assert isinstance(role, dict)
                    rolename = 'test_role_{}'.format(idx)
                    try:
                        cls._ceph_cmd(['dashboard', 'ac-role-show', rolename])
                        cls._ceph_cmd(['dashboard', 'ac-role-delete', rolename])
                    except CommandFailedError as ex:
                        if ex.exitstatus != 2:
                            raise ex
                    cls._ceph_cmd(['dashboard', 'ac-role-create', rolename])
                    for mod, perms in role.items():
                        args = ['dashboard', 'ac-role-add-scope-perms', rolename, mod]
                        args.extend(perms)
                        cls._ceph_cmd(args)
                    set_roles_args.append(rolename)
            cls._ceph_cmd(set_roles_args)

    @classmethod
    def login(cls, username, password):
        if cls._loggedin:
            cls.logout()
        cls._post('/api/auth', {'username': username, 'password': password})
        cls._assertEq(cls._resp.status_code, 201)
        cls._token = cls.jsonBody()['token']
        cls._loggedin = True

    @classmethod
    def logout(cls):
        if cls._loggedin:
            cls._post('/api/auth/logout')
            cls._assertEq(cls._resp.status_code, 200)
            cls._token = None
            cls._loggedin = False

    @classmethod
    def delete_user(cls, username, roles=None):
        if roles is None:
            roles = []
        cls._ceph_cmd(['dashboard', 'ac-user-delete', username])
        for idx, role in enumerate(roles):
            if isinstance(role, dict):
                cls._ceph_cmd(['dashboard', 'ac-role-delete', 'test_role_{}'.format(idx)])

    @classmethod
    def RunAs(cls, username, password, roles=None, force_password=True,
              cmd_args=None, login=True):
        def wrapper(func):
            def execute(self, *args, **kwargs):
                self.create_user(username, password, roles,
                                 force_password, cmd_args)
                if login:
                    self.login(username, password)
                res = func(self, *args, **kwargs)
                if login:
                    self.logout()
                self.delete_user(username, roles)
                return res

            return execute

        return wrapper

    @classmethod
    def set_jwt_token(cls, token):
        cls._token = token

    @classmethod
    def setUpClass(cls):
        super(DashboardTestCase, cls).setUpClass()
        cls._assign_ports("dashboard", "ssl_server_port")
        cls._load_module("dashboard")
        cls._base_uri = cls._get_uri("dashboard").rstrip('/')

        if cls.CEPHFS:
            cls.mds_cluster.clear_firewall()

            # To avoid any issues with e.g. unlink bugs, we destroy and recreate
            # the filesystem rather than just doing a rm -rf of files
            cls.mds_cluster.mds_stop()
            cls.mds_cluster.mds_fail()
            cls.mds_cluster.delete_all_filesystems()
            cls.fs = None  # is now invalid!

            cls.fs = cls.mds_cluster.newfs(create=True)
            cls.fs.mds_restart()

            # In case some test messed with auth caps, reset them
            # pylint: disable=not-an-iterable
            client_mount_ids = [m.client_id for m in cls.mounts]
            for client_id in client_mount_ids:
                cls.mds_cluster.mon_manager.raw_cluster_cmd_result(
                    'auth', 'caps', "client.{0}".format(client_id),
                    'mds', 'allow',
                    'mon', 'allow r',
                    'osd', 'allow rw pool={0}'.format(cls.fs.get_data_pool_name()))

            # wait for mds restart to complete...
            cls.fs.wait_for_daemons()

        cls._token = None
        cls._session = requests.Session()
        cls._resp = None

        cls.create_user('admin', 'admin', cls.AUTH_ROLES)
        if cls.AUTO_AUTHENTICATE:
            cls.login('admin', 'admin')

    def setUp(self):
        super(DashboardTestCase, self).setUp()
        if not self._loggedin and self.AUTO_AUTHENTICATE:
            self.login('admin', 'admin')
        self.wait_for_health_clear(self.TIMEOUT_HEALTH_CLEAR)

    @classmethod
    def tearDownClass(cls):
        super(DashboardTestCase, cls).tearDownClass()

    # pylint: disable=inconsistent-return-statements
    @classmethod
    def _request(cls, url, method, data=None, params=None):
        url = "{}{}".format(cls._base_uri, url)
        log.info("Request %s to %s", method, url)
        headers = {}
        if cls._token:
            headers['Authorization'] = "Bearer {}".format(cls._token)

        if method == 'GET':
            cls._resp = cls._session.get(url, params=params, verify=False,
                                         headers=headers)
        elif method == 'POST':
            cls._resp = cls._session.post(url, json=data, params=params,
                                          verify=False, headers=headers)
        elif method == 'DELETE':
            cls._resp = cls._session.delete(url, json=data, params=params,
                                            verify=False, headers=headers)
        elif method == 'PUT':
            cls._resp = cls._session.put(url, json=data, params=params,
                                         verify=False, headers=headers)
        else:
            assert False
        try:
            if not cls._resp.ok:
                # Output response for easier debugging.
                log.error("Request response: %s", cls._resp.text)
            content_type = cls._resp.headers['content-type']
            if content_type == 'application/json' and cls._resp.text and cls._resp.text != "":
                return cls._resp.json()
            return cls._resp.text
        except ValueError as ex:
            log.exception("Failed to decode response: %s", cls._resp.text)
            raise ex

    @classmethod
    def _get(cls, url, params=None):
        return cls._request(url, 'GET', params=params)

    @classmethod
    def _view_cache_get(cls, url, retries=5):
        retry = True
        while retry and retries > 0:
            retry = False
            res = cls._get(url)
            if isinstance(res, dict):
                res = [res]
            for view in res:
                assert 'value' in view
                if not view['value']:
                    retry = True
            retries -= 1
        if retries == 0:
            raise Exception("{} view cache exceeded number of retries={}"
                            .format(url, retries))
        return res

    @classmethod
    def _post(cls, url, data=None, params=None):
        cls._request(url, 'POST', data, params)

    @classmethod
    def _delete(cls, url, data=None, params=None):
        cls._request(url, 'DELETE', data, params)

    @classmethod
    def _put(cls, url, data=None, params=None):
        cls._request(url, 'PUT', data, params)

    @classmethod
    def _assertEq(cls, v1, v2):
        if not v1 == v2:
            raise Exception("assertion failed: {} != {}".format(v1, v2))

    @classmethod
    def _assertIn(cls, v1, v2):
        if v1 not in v2:
            raise Exception("assertion failed: {} not in {}".format(v1, v2))

    @classmethod
    def _assertIsInst(cls, v1, v2):
        if not isinstance(v1, v2):
            raise Exception("assertion failed: {} not instance of {}".format(v1, v2))

    # pylint: disable=too-many-arguments
    @classmethod
    def _task_request(cls, method, url, data, timeout):
        res = cls._request(url, method, data)
        cls._assertIn(cls._resp.status_code, [200, 201, 202, 204, 400, 403, 404])

        if cls._resp.status_code == 403:
            return None

        if cls._resp.status_code != 202:
            log.info("task finished immediately")
            return res

        cls._assertIn('name', res)
        cls._assertIn('metadata', res)
        task_name = res['name']
        task_metadata = res['metadata']

        retries = int(timeout)
        res_task = None
        while retries > 0 and not res_task:
            retries -= 1
            log.info("task (%s, %s) is still executing", task_name,
                     task_metadata)
            time.sleep(1)
            _res = cls._get('/api/task?name={}'.format(task_name))
            cls._assertEq(cls._resp.status_code, 200)
            executing_tasks = [task for task in _res['executing_tasks'] if
                               task['metadata'] == task_metadata]
            finished_tasks = [task for task in _res['finished_tasks'] if
                               task['metadata'] == task_metadata]
            if not executing_tasks and finished_tasks:
                res_task = finished_tasks[0]

        if retries <= 0:
            raise Exception("Waiting for task ({}, {}) to finish timed out. {}"
                            .format(task_name, task_metadata, _res))

        log.info("task (%s, %s) finished", task_name, task_metadata)
        if res_task['success']:
            if method == 'POST':
                cls._resp.status_code = 201
            elif method == 'PUT':
                cls._resp.status_code = 200
            elif method == 'DELETE':
                cls._resp.status_code = 204
            return res_task['ret_value']
        else:
            if 'status' in res_task['exception']:
                cls._resp.status_code = res_task['exception']['status']
            else:
                cls._resp.status_code = 500
            return res_task['exception']

    @classmethod
    def _task_post(cls, url, data=None, timeout=60):
        return cls._task_request('POST', url, data, timeout)

    @classmethod
    def _task_delete(cls, url, timeout=60):
        return cls._task_request('DELETE', url, None, timeout)

    @classmethod
    def _task_put(cls, url, data=None, timeout=60):
        return cls._task_request('PUT', url, data, timeout)

    @classmethod
    def cookies(cls):
        return cls._resp.cookies

    @classmethod
    def jsonBody(cls):
        return cls._resp.json()

    @classmethod
    def reset_session(cls):
        cls._session = requests.Session()

    def assertSubset(self, data, biggerData):
        for key, value in data.items():
            self.assertEqual(biggerData[key], value)

    def assertJsonBody(self, data):
        body = self._resp.json()
        self.assertEqual(body, data)

    def assertJsonSubset(self, data):
        self.assertSubset(data, self._resp.json())

    def assertSchema(self, data, schema):
        try:
            return _validate_json(data, schema)
        except _ValError as e:
            self.assertEqual(data, str(e))

    def assertSchemaBody(self, schema):
        self.assertSchema(self.jsonBody(), schema)

    def assertBody(self, body):
        self.assertEqual(self._resp.text, body)

    def assertStatus(self, status):
        if isinstance(status, list):
            self.assertIn(self._resp.status_code, status)
        else:
            self.assertEqual(self._resp.status_code, status)

    def assertHeaders(self, headers):
        for name, value in headers.items():
            self.assertIn(name, self._resp.headers)
            self.assertEqual(self._resp.headers[name], value)

    def assertError(self, code=None, component=None, detail=None):
        body = self._resp.json()
        if code:
            self.assertEqual(body['code'], code)
        if component:
            self.assertEqual(body['component'], component)
        if detail:
            self.assertEqual(body['detail'], detail)

    @classmethod
    def _ceph_cmd(cls, cmd):
        res = cls.mgr_cluster.mon_manager.raw_cluster_cmd(*cmd)
        log.info("command result: %s", res)
        return res

    @classmethod
    def _ceph_cmd_result(cls, cmd):
        exitstatus = cls.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd)
        log.info("command exit status: %d", exitstatus)
        return exitstatus

    def set_config_key(self, key, value):
        self._ceph_cmd(['config-key', 'set', key, value])

    def get_config_key(self, key):
        return self._ceph_cmd(['config-key', 'get', key])

    @classmethod
    def _cmd(cls, args):
        return cls.mgr_cluster.admin_remote.run(args=args)

    @classmethod
    def _rbd_cmd(cls, cmd):
        args = ['rbd']
        args.extend(cmd)
        cls._cmd(args)

    @classmethod
    def _radosgw_admin_cmd(cls, cmd):
        args = ['radosgw-admin']
        args.extend(cmd)
        cls._cmd(args)

    @classmethod
    def _rados_cmd(cls, cmd):
        args = ['rados']
        args.extend(cmd)
        cls._cmd(args)

    @classmethod
    def mons(cls):
        out = cls.ceph_cluster.mon_manager.raw_cluster_cmd('quorum_status')
        j = json.loads(out)
        return [mon['name'] for mon in j['monmap']['mons']]

    @classmethod
    def find_object_in_list(cls, key, value, iterable):
        """
        Get the first occurrence of an object within a list with
        the specified key/value.
        :param key: The name of the key.
        :param value: The value to search for.
        :param iterable: The list to process.
        :return: Returns the found object or None.
        """
        for obj in iterable:
            if key in obj and obj[key] == value:
                return obj
        return None


class JLeaf(namedtuple('JLeaf', ['typ', 'none'])):
    def __new__(cls, typ, none=False):
        if typ == str:
            typ = six.string_types
        return super(JLeaf, cls).__new__(cls, typ, none)


JList = namedtuple('JList', ['elem_typ'])

JTuple = namedtuple('JList', ['elem_typs'])

JUnion = namedtuple('JUnion', ['elem_typs'])

class JObj(namedtuple('JObj', ['sub_elems', 'allow_unknown', 'none', 'unknown_schema'])):
    def __new__(cls, sub_elems, allow_unknown=False, none=False, unknown_schema=None):
        """
        :type sub_elems: dict[str, JAny | JLeaf | JList | JObj | type]
        :type allow_unknown: bool
        :type none: bool
        :type unknown_schema: int, str, JAny | JLeaf | JList | JObj
        :return:
        """
        return super(JObj, cls).__new__(cls, sub_elems, allow_unknown, none, unknown_schema)


JAny = namedtuple('JAny', ['none'])


class _ValError(Exception):
    def __init__(self, msg, path):
        path_str = ''.join('[{}]'.format(repr(p)) for p in path)
        super(_ValError, self).__init__('In `input{}`: {}'.format(path_str, msg))


# pylint: disable=dangerous-default-value,inconsistent-return-statements
def _validate_json(val, schema, path=[]):
    """
    >>> d = {'a': 1, 'b': 'x', 'c': range(10)}
    ... ds = JObj({'a': int, 'b': str, 'c': JList(int)})
    ... _validate_json(d, ds)
    True
    >>> _validate_json({'num': 1}, JObj({'num': JUnion([int,float])}))
    True
    >>> _validate_json({'num': 'a'}, JObj({'num': JUnion([int,float])}))
    False
    """
    if isinstance(schema, JAny):
        if not schema.none and val is None:
            raise _ValError('val is None', path)
        return True
    if isinstance(schema, JLeaf):
        if schema.none and val is None:
            return True
        if not isinstance(val, schema.typ):
            raise _ValError('val not of type {}'.format(schema.typ), path)
        return True
    if isinstance(schema, JList):
        if not isinstance(val, list):
            raise _ValError('val="{}" is not a list'.format(val), path)
        return all(_validate_json(e, schema.elem_typ, path + [i]) for i, e in enumerate(val))
    if isinstance(schema, JTuple):
        return all(_validate_json(val[i], typ, path + [i])
                   for i, typ in enumerate(schema.elem_typs))
    if isinstance(schema, JUnion):
        for typ in schema.elem_typs:
            try:
                if _validate_json(val, typ, path):
                    return True
            except _ValError:
                pass
        return False
    if isinstance(schema, JObj):
        if val is None and schema.none:
            return True
        elif val is None:
            raise _ValError('val is None', path)
        if not hasattr(val, 'keys'):
            raise _ValError('val="{}" is not a dict'.format(val), path)
        missing_keys = set(schema.sub_elems.keys()).difference(set(val.keys()))
        if missing_keys:
            raise _ValError('missing keys: {}'.format(missing_keys), path)
        unknown_keys = set(val.keys()).difference(set(schema.sub_elems.keys()))
        if not schema.allow_unknown and unknown_keys:
            raise _ValError('unknown keys: {}'.format(unknown_keys), path)
        result = all(
            _validate_json(val[key], sub_schema, path + [key])
            for key, sub_schema in schema.sub_elems.items()
        )
        if unknown_keys and schema.allow_unknown and schema.unknown_schema:
            result += all(
                _validate_json(val[key], schema.unknown_schema, path + [key])
                for key in unknown_keys
            )
        return result
    if schema in [str, int, float, bool, six.string_types]:
        return _validate_json(val, JLeaf(schema), path)

    assert False, str(path)
