# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import json
import logging
from collections import namedtuple

import requests
import six

from ..mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


def authenticate(func):
    def decorate(self, *args, **kwargs):
        self._ceph_cmd(['dashboard', 'set-login-credentials', 'admin', 'admin'])
        self._post('/api/auth', {'username': 'admin', 'password': 'admin'})
        self.assertStatus(201)
        return func(self, *args, **kwargs)
    return decorate


class DashboardTestCase(MgrTestCase):
    MGRS_REQUIRED = 2
    MDSS_REQUIRED = 1
    REQUIRE_FILESYSTEM = True
    CLIENTS_REQUIRED = 1
    CEPHFS = False

    _session = None
    _resp = None

    @classmethod
    def setUpClass(cls):
        super(DashboardTestCase, cls).setUpClass()
        cls._assign_ports("dashboard", "server_port")
        cls._load_module("dashboard")
        cls.base_uri = cls._get_uri("dashboard").rstrip('/')

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

        cls._session = requests.Session()
        cls._resp = None

    @classmethod
    def tearDownClass(cls):
        super(DashboardTestCase, cls).tearDownClass()

    # pylint: disable=inconsistent-return-statements
    @classmethod
    def _request(cls, url, method, data=None, params=None):
        url = "{}{}".format(cls.base_uri, url)
        log.info("request %s to %s", method, url)
        if method == 'GET':
            cls._resp = cls._session.get(url, params=params)
            try:
                return cls._resp.json()
            except ValueError as ex:
                log.exception("Failed to decode response: %s", cls._resp.text)
                raise ex
        elif method == 'POST':
            cls._resp = cls._session.post(url, json=data, params=params)
        elif method == 'DELETE':
            cls._resp = cls._session.delete(url, json=data, params=params)
        elif method == 'PUT':
            cls._resp = cls._session.put(url, json=data, params=params)
        else:
            assert False
        return None

    @classmethod
    def _get(cls, url, params=None):
        return cls._request(url, 'GET', params=params)

    @classmethod
    def _get_view_cache(cls, url, retries=5):
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
    def cookies(cls):
        return cls._resp.cookies

    @classmethod
    def jsonBody(cls):
        return cls._resp.json()

    @classmethod
    def reset_session(cls):
        cls._session = requests.Session()

    def assertJsonBody(self, data):
        body = self._resp.json()
        self.assertEqual(body, data)

    def assertSchema(self, data, schema):
        try:
            return _validate_json(data, schema)
        except _ValError as e:
            self.assertEqual(data, str(e))

    def assertBody(self, body):
        self.assertEqual(self._resp.text, body)

    def assertStatus(self, status):
        self.assertEqual(self._resp.status_code, status)

    @classmethod
    def _ceph_cmd(cls, cmd):
        res = cls.mgr_cluster.mon_manager.raw_cluster_cmd(*cmd)
        log.info("command result: %s", res)
        return res

    def set_config_key(self, key, value):
        self._ceph_cmd(['config-key', 'set', key, value])

    def get_config_key(self, key):
        return self._ceph_cmd(['config-key', 'get', key])

    @classmethod
    def _rbd_cmd(cls, cmd):
        args = [
            'rbd'
        ]
        args.extend(cmd)
        cls.mgr_cluster.admin_remote.run(args=args)

    @classmethod
    def _radosgw_admin_cmd(cls, cmd):
        args = [
            'radosgw-admin'
        ]
        args.extend(cmd)
        cls.mgr_cluster.admin_remote.run(args=args)

    @classmethod
    def mons(cls):
        out = cls.ceph_cluster.mon_manager.raw_cluster_cmd('mon_status')
        j = json.loads(out)
        return [mon['name'] for mon in j['monmap']['mons']]


class JLeaf(namedtuple('JLeaf', ['typ', 'none'])):
    def __new__(cls, typ, none=False):
        if typ == str:
            typ = six.string_types
        return super(JLeaf, cls).__new__(cls, typ, none)


JList = namedtuple('JList', ['elem_typ'])

JTuple = namedtuple('JList', ['elem_typs'])


class JObj(namedtuple('JObj', ['sub_elems', 'allow_unknown'])):
    def __new__(cls, sub_elems, allow_unknown=False):
        """
        :type sub_elems: dict[str, JAny | JLeaf | JList | JObj]
        :type allow_unknown: bool
        :return:
        """
        return super(JObj, cls).__new__(cls, sub_elems, allow_unknown)


JAny = namedtuple('JAny', ['none'])


class _ValError(Exception):
    def __init__(self, msg, path):
        path_str = ''.join('[{}]'.format(repr(p)) for p in path)
        super(_ValError, self).__init__('In `input{}`: {}'.format(path_str, msg))


# pylint: disable=dangerous-default-value,inconsistent-return-statements
def _validate_json(val, schema, path=[]):
    """
    >>> d = {'a': 1, 'b': 'x', 'c': range(10)}
    ... ds = JObj({'a': JLeaf(int), 'b': JLeaf(str), 'c': JList(JLeaf(int))})
    ... _validate_json(d, ds)
    True
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
        return all(_validate_json(e, schema.elem_typ, path + [i]) for i, e in enumerate(val))
    if isinstance(schema, JTuple):
        return all(_validate_json(val[i], typ, path + [i])
                   for i, typ in enumerate(schema.elem_typs))
    if isinstance(schema, JObj):
        missing_keys = set(schema.sub_elems.keys()).difference(set(val.keys()))
        if missing_keys:
            raise _ValError('missing keys: {}'.format(missing_keys), path)
        unknown_keys = set(val.keys()).difference(set(schema.sub_elems.keys()))
        if not schema.allow_unknown and unknown_keys:
            raise _ValError('unknown keys: {}'.format(unknown_keys), path)
        return all(
            _validate_json(val[sub_elem_name], sub_elem, path + [sub_elem_name])
            for sub_elem_name, sub_elem in schema.sub_elems.items()
        )

    assert False, str(path)
