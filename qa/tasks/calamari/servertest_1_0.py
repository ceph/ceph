#!/usr/bin/env python

import datetime
import os
import logging
import logging.handlers
import requests
import uuid
import unittest
from http_client import AuthenticatedHttpClient

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

global base_uri
global client
base_uri = None
server_uri = None
client = None

def setUpModule():
    global base_uri
    global server_uri
    global client
    try:
        base_uri = os.environ['CALAMARI_BASE_URI']
    except KeyError:
        log.error('Must define CALAMARI_BASE_URI')
        os._exit(1)
    if not base_uri.endswith('/'):
        base_uri += '/'
    if not base_uri.endswith('api/v1/'):
        base_uri += 'api/v1/'
    client = AuthenticatedHttpClient(base_uri, 'admin', 'admin')
    server_uri = base_uri.replace('api/v1/', '')
    client.login()

class RestTest(unittest.TestCase):
    'Base class for all tests here; get class\'s data'

    def setUp(self):
        # Called once for each test_* case.  A bit wasteful, but we
        # really like using the simple class variable self.uri
        # to customize each derived TestCase
        method = getattr(self, 'method', 'GET')
        raw = self.uri.startswith('/')
        self.response = self.get_object(method, self.uri, raw=raw)

    def get_object(self, method, url, raw=False):
        global server_uri
        'Return Python object decoded from JSON response to method/url'
        if not raw:
            return client.request(method, url).json()
        else:
            return requests.request(method, server_uri + url).json()

class TestUserMe(RestTest):

    uri = 'user/me'

    def test_me(self):
        self.assertEqual(self.response['username'], 'admin')

class TestCluster(RestTest):

    uri = 'cluster'

    def test_id(self):
        self.assertEqual(self.response[0]['id'], 1)

    def test_times(self):
        for time in (
            self.response[0]['cluster_update_time'],
            self.response[0]['cluster_update_attempt_time'],
        ):
            self.assertTrue(is_datetime(time))

    def test_api_base_url(self):
        api_base_url = self.response[0]['api_base_url']
        self.assertTrue(api_base_url.startswith('http'))
        self.assertIn('api/v0.1', api_base_url)

class TestHealth(RestTest):

    uri = 'cluster/1/health'

    def test_cluster(self):
        self.assertEqual(self.response['cluster'], 1)

    def test_times(self):
        for time in (
            self.response['cluster_update_time'],
            self.response['added'],
        ):
            self.assertTrue(is_datetime(time))

    def test_report_and_overall_status(self):
        self.assertIn('report', self.response)
        self.assertIn('overall_status', self.response['report'])

class TestHealthCounters(RestTest):

    uri = 'cluster/1/health_counters'

    def test_cluster(self):
        self.assertEqual(self.response['cluster'], 1)

    def test_time(self):
        self.assertTrue(is_datetime(self.response['cluster_update_time']))

    def test_existence(self):
        for section in ('pg', 'mon', 'osd'):
            for counter in ('warn', 'critical', 'ok'):
                count = self.response[section][counter]['count']
                self.assertIsInstance(count, int)
        self.assertIsInstance(self.response['pool']['total'], int)

    def test_mds_sum(self):
        count = self.response['mds']
        self.assertEqual(
            count['up_not_in'] + count['not_up_not_in'] + count['up_in'],
            count['total']
        )

class TestSpace(RestTest):

    uri = 'cluster/1/space'

    def test_cluster(self):
        self.assertEqual(self.response['cluster'], 1)

    def test_times(self):
        for time in (
            self.response['cluster_update_time'],
            self.response['added'],
        ):
            self.assertTrue(is_datetime(time))

    def test_space(self):
        for size in ('free_bytes', 'used_bytes', 'capacity_bytes'):
            self.assertIsInstance(self.response['space'][size], int)
            self.assertGreater(self.response['space'][size], 0)

    def test_report(self):
        for size in ('total_used', 'total_space', 'total_avail'):
            self.assertIsInstance(self.response['report'][size], int)
            self.assertGreater(self.response['report'][size], 0)

class TestOSD(RestTest):

    uri = 'cluster/1/osd'

    def test_cluster(self):
        self.assertEqual(self.response['cluster'], 1)

    def test_times(self):
        for time in (
            self.response['cluster_update_time'],
            self.response['added'],
        ):
            self.assertTrue(is_datetime(time))

    def test_osd_uuid(self):
        for osd in self.response['osds']:
            uuidobj = uuid.UUID(osd['uuid'])
            self.assertEqual(str(uuidobj), osd['uuid'])

    def test_osd_pools(self):
        for osd in self.response['osds']:
            if osd['up'] != 1:
                continue
            self.assertIsInstance(osd['pools'], list)
            self.assertIsInstance(osd['pools'][0], basestring)

    def test_osd_up_in(self):
        for osd in self.response['osds']:
            for flag in ('up', 'in'):
                self.assertIn(osd[flag], (0, 1))

    def test_osd_0(self):
        osd0 = self.get_object('GET', 'cluster/1/osd/0')['osd']
        for field in osd0.keys():
            if not field.startswith('cluster_update_time'):
                self.assertEqual(self.response['osds'][0][field], osd0[field])

class TestPool(RestTest):

    uri = 'cluster/1/pool'

    def test_cluster(self):
        for pool in self.response:
            self.assertEqual(pool['cluster'], 1)

    def test_fields_are_ints(self):
        for pool in self.response:
            for field in ('id', 'used_objects', 'used_bytes'):
                self.assertIsInstance(pool[field], int)

    def test_name_is_str(self):
        for pool in self.response:
            self.assertIsInstance(pool['name'], basestring)

    def test_pool_0(self):
        poolid = self.response[0]['id']
        pool = self.get_object('GET', 'cluster/1/pool/{id}'.format(id=poolid))
        self.assertEqual(self.response[0], pool)

class TestServer(RestTest):

    uri = 'cluster/1/server'

    def test_ipaddr(self):
        for server in self.response:
            octets = server['addr'].split('.')
            self.assertEqual(len(octets), 4)
            for octetstr in octets:
                octet = int(octetstr)
                self.assertIsInstance(octet, int)
                self.assertGreaterEqual(octet, 0)
                self.assertLessEqual(octet, 255)

    def test_hostname_name_strings(self):
        for server in self.response:
            for field in ('name', 'hostname'):
                self.assertIsInstance(server[field], basestring)

    def test_services(self):
        for server in self.response:
            self.assertIsInstance(server['services'], list)
            for service in server['services']:
                self.assertIn(service['type'], ('osd', 'mon', 'mds'))

class TestGraphitePoolIOPS(RestTest):

    uri = '/graphite/render?format=json-array&' \
          'target=ceph.cluster.ceph.pool.0.num_read&' \
          'target=ceph.cluster.ceph.pool.0.num_write'

    def test_targets_contain_request(self):
        self.assertIn('targets', self.response)
        self.assertIn('ceph.cluster.ceph.pool.0.num_read',
                      self.response['targets'])
        self.assertIn('ceph.cluster.ceph.pool.0.num_write',
                      self.response['targets'])

    def test_datapoints(self):
        self.assertIn('datapoints', self.response)
        self.assertGreater(len(self.response['datapoints']), 0)
        data = self.response['datapoints'][0]
        self.assertEqual(len(data), 3)
        self.assertIsInstance(data[0], int)
        if data[1]:
            self.assertIsInstance(data[1], float)
        if data[2]:
            self.assertIsInstance(data[2], float)

#
# Utility functions
#

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

def is_datetime(time):
    datetime.datetime.strptime(time, DATETIME_FORMAT)
    return True

if __name__ == '__main__':
    unittest.main()
