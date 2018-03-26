# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import json
import logging
import os
import subprocess
import sys
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

    @classmethod
    def tearDownClass(cls):
        super(DashboardTestCase, cls).tearDownClass()

    def __init__(self, *args, **kwargs):
        super(DashboardTestCase, self).__init__(*args, **kwargs)
        self._session = requests.Session()
        self._resp = None

    def _request(self, url, method, data=None):
        url = "{}{}".format(self.base_uri, url)
        log.info("request %s to %s", method, url)
        if method == 'GET':
            self._resp = self._session.get(url)
            try:
                return self._resp.json()
            except ValueError as ex:
                log.exception("Failed to decode response: %s", self._resp.text)
                raise ex
        elif method == 'POST':
            self._resp = self._session.post(url, json=data)
        elif method == 'DELETE':
            self._resp = self._session.delete(url, json=data)
        elif method == 'PUT':
            self._resp = self._session.put(url, json=data)
        else:
            assert False

    def _get(self, url):
        return self._request(url, 'GET')

    def _post(self, url, data=None):
        self._request(url, 'POST', data)

    def _delete(self, url, data=None):
        self._request(url, 'DELETE', data)

    def _put(self, url, data=None):
        self._request(url, 'PUT', data)

    def cookies(self):
        return self._resp.cookies

    def jsonBody(self):
        return self._resp.json()

    def reset_session(self):
        self._session = requests.Session()

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



