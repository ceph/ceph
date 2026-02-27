#!/usr/bin/env python3

# Copyright (C) 2025 Binero
# Author: Tobias Urdin <tobias.urdin@binero.com>

import boto3
from botocore.auth import HmacV1Auth
from botocore.awsrequest import AWSRequest
from botocore.compat import (parse_qsl, urlparse)
import json
import requests
import subprocess
import unittest
from urllib.parse import urljoin

import typing as ty


class RGWAdminException(Exception):
    def __init__(self, r: subprocess.CompletedProcess):
        message = (
            f"radosgw-admin command with args {str(r.args)} failed, "
            f"return code: {r.returncode} stdout: "
            f"{str(r.stdout)} stderr: {str(r.stderr)}"
        )
        super().__init__(message)


class RGWUserNotFound(Exception):
    pass


class AWSAuth(requests.auth.AuthBase):
    def __init__(self, session=None):
        self.session = session or boto3.Session()
        self.sig = HmacV1Auth(
            credentials=self.session.get_credentials(),
        )

    def __call__(self, request):
        url = urlparse(request.url)
        awsrequest = AWSRequest(
            method=request.method,
            url=f'{url.scheme}://{url.netloc}{url.path}',
            data=request.body,
            params=dict(parse_qsl(url.query)),
        )
        self.sig.add_auth(awsrequest)
        for key, val in request.headers.items():
            if key not in awsrequest.headers:
                awsrequest.headers[key] = val
        return awsrequest.prepare()


class TestRGWAdminHelper:
    def __init__(self) -> None:
        self.radosgw_admin = 'radosgw-admin'

    def _run_radosgw_admin(
            self, args: ty.List[str]) -> subprocess.CompletedProcess:
        """Run radosgw-admin command."""
        cmd = [self.radosgw_admin, '--format=json']
        cmd += args
        return subprocess.run(cmd, capture_output=True)

    def _json(self, r: subprocess.CompletedProcess) -> ty.Any:
        """Decode and parse JSON data."""
        data = r.stdout.decode('utf-8')
        return json.loads(data)

    def get_rgw_user(self, uid: str) -> ty.Dict:
        """Get a RGW user using radosgw-admin."""
        r = self._run_radosgw_admin(['user', 'info', f'--uid={uid}'])
        if r.returncode == 22:
            raise RGWUserNotFound()
        if r.returncode != 0:
            raise RGWAdminException(r)

        return self._json(r)

    def create_rgw_user(
        self, uid: str, display_name: str, caps: str = ""
    ) -> ty.Dict:
        """Create a RGW user using radosgw-admin."""
        args = [
            'user', 'create', f'--uid={uid}',
            f'--display-name={display_name}',
        ]
        if caps != "":
            args += [f'--caps={caps}']

        r = self._run_radosgw_admin(args)
        if r.returncode != 0:
            raise RGWAdminException(r)

        return self._json(r)

    def get_or_create_rgw_user(
        self, uid: str, display_name: str, caps: str = ""
    ) -> ty.Dict:
        """Get or create RGW user using radosgw-admin."""
        try:
            return self.get_rgw_user(uid)
        except RGWUserNotFound:
            return self.create_rgw_user(uid, display_name, caps)

    def set_user_max_buckets(self, uid: str, max_buckets: int) -> None:
        """Set max-buckets for a RGW user by uid."""
        args = [
            'user', 'modify', f'--uid={uid}',
            f'--max-buckets={max_buckets}'
        ]
        r = self._run_radosgw_admin(args)
        if r.returncode != 0:
            raise RGWAdminException(r)

    def get_boto3_session(self, user: ty.Dict) -> boto3.session.Session:
        """Get a boto3 session for a RGW user."""
        assert 'keys' in user
        assert len(user['keys']) == 1
        assert 'access_key' in user['keys'][0]
        assert 'secret_key' in user['keys'][0]

        return boto3.session.Session(
            aws_access_key_id=user['keys'][0]['access_key'],
            aws_secret_access_key=user['keys'][0]['secret_key'])

    def get_boto3_client(self, session: boto3.session.Session):
        """Get a boto3 s3 client from a session."""
        def _try_client(portnum: str, ssl: bool, proto: str):
            endpoint = proto + '://localhost:' + portnum
            client = session.client(
                's3', use_ssl=ssl, endpoint_url=endpoint, verify=False)
            list(client.list_buckets(MaxBuckets=1))
            return endpoint, client

        try:
            return _try_client('80', False, 'http')
        except Exception:
            try:
                return _try_client('8000', False, 'http')
            except Exception:
                return _try_client('443', True, 'https')


class TestRGWAdminBucket(unittest.TestCase):
    helper: ty.ClassVar[TestRGWAdminHelper]
    admin_user: ty.ClassVar[ty.Dict]
    admin_session: ty.ClassVar[boto3.session.Session]
    session: ty.ClassVar[requests.Session]
    test_user: ty.ClassVar[ty.Dict]
    test_session: ty.ClassVar[boto3.session.Session]
    endpoint: ty.ClassVar[str]
    test_client: ty.ClassVar[ty.Any]
    params: ty.ClassVar[ty.Dict]
    expected_buckets: ty.ClassVar[ty.List[str]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.helper = TestRGWAdminHelper()

        admin_caps = "usage=read;metadata=read;users=read;buckets=read"
        cls.admin_user = cls.helper.get_or_create_rgw_user(
            'adminbucket_admin', 'Admin Bucket User', admin_caps)

        cls.admin_session = cls.helper.get_boto3_session(
            cls.admin_user)

        cls.session = requests.Session()
        cls.session.verify = False
        cls.session.auth = AWSAuth(cls.admin_session)

        test_user_uid = 'adminbucket_user'
        cls.test_user = cls.helper.get_or_create_rgw_user(
            test_user_uid, 'Admin Bucket Test User')
        cls.helper.set_user_max_buckets(test_user_uid, 2000)

        cls.test_session = cls.helper.get_boto3_session(
            cls.test_user)
        cls.endpoint, cls.test_client = cls.helper.get_boto3_client(
            cls.test_session)

        cls._populate_buckets()

        cls.params = {
            'uid': test_user_uid,
            'stats': False,
        }

    @classmethod
    def _populate_buckets(cls) -> None:
        """
        Populate 1500 buckets for the test user.
        """
        num_test_buckets = 1500

        resp = cls.test_client.list_buckets()
        num_buckets = len(resp['Buckets'])

        print(f'Number of buckets: {num_buckets}')
        assert num_buckets == 0 or num_buckets == num_test_buckets

        if num_buckets == 0:
            print(
                f'Populating {num_test_buckets} buckets for test user...')

            for i in range(1, num_test_buckets + 1):
                bucket_name = f"test-bucket-{i}"
                cls.test_client.create_bucket(Bucket=bucket_name)

        # Populate a list of expected bucket
        cls.expected_buckets = [f"test-bucket-{i}" for i in
                                range(1, num_test_buckets + 1)]

    def _get_url(
        self, path: str = '', params: ty.Optional[ty.Dict] = None
    ) -> ty.Dict:
        """
        Prepare HTTP URL and do a authenticated HTTP GET request
        to the URL, raise exception based on HTTP status code and
        if ok return data parsed from JSON response.
        """
        url = urljoin(self.endpoint, path)
        r = self.session.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def _test_original_bucket_list(self, stats: bool = False) -> None:
        # Expects original format without stats:
        # [
        #   "bucket-1",
        #   "bucket-2"
        #   ...
        # ]
        # Expects original format with stats:
        # [
        #   {"bucket": "bucket-1", ...},
        #   {"bucket": "bucket-2", ...}
        #   ...
        # ]
        params = self.params.copy()
        params['stats'] = stats
        r = self._get_url('/admin/bucket', params)
        self.assertEqual(len(r), len(self.expected_buckets))

    def test_original_bucket_list_without_stats(self) -> None:
        self._test_original_bucket_list(stats=False)

    def test_original_bucket_list_with_stats(self) -> None:
        self._test_original_bucket_list(stats=True)

    def _test_bucket_list_max_entries(self, stats: bool = False) -> None:
        # Expects new format:
        # {
        #   "buckets": [
        #     "bucket-1",
        #     "bucket-2",
        #     ...
        #   ],
        #   "count": 44,
        #   "truncated": true,
        #   "marker": "bucket-44"
        # }
        params = self.params.copy()
        params['stats'] = stats
        params['max-entries'] = 123
        r = self._get_url('/admin/bucket', params)
        for key in ['buckets', 'count', 'truncated', 'marker']:
            self.assertIn(key, r)
        self.assertEqual(len(r['buckets']), params['max-entries'])
        self.assertEqual(r['count'], params['max-entries'])
        self.assertTrue(r['truncated'])
        if stats:
            marker_bucket = r['buckets'][-1]['bucket']
        else:
            marker_bucket = r['buckets'][-1]
        self.assertEqual(r['marker'], marker_bucket)

    def test_bucket_list_max_entries_without_stats(self) -> None:
        self._test_bucket_list_max_entries(stats=False)

    def test_bucket_list_max_entries_with_stats(self) -> None:
        self._test_bucket_list_max_entries(stats=True)

    def test_bucket_list_max_entries_capped(self) -> None:
        """
        Test that max-entries > 1000 works when the RGW admin
        API in the background will restrict max_items to
        rgw_list_buckets_max_chunk (that defaults to 1000).
        """
        params = self.params.copy()
        params['max-entries'] = 1200
        r = self._get_url('/admin/bucket', params)
        self.assertEqual(len(r['buckets']), 1200)
        self.assertEqual(r['count'], 1200)
        # Verify that truncated is indeed true since first
        # iteration in backend would return 1000 buckets and
        # the next iteration should only return 200 and say
        # it's truncated and not read up the next 1000 buckets.
        self.assertTrue(r['truncated'])
        self.assertIn('marker', r)

    def test_bucket_list_max_entries_negative(self) -> None:
        """
        Test with a negative max-entries should ignore max-entries
        completely and return everything.
        """
        params = self.params.copy()
        params['max-entries'] = -400
        r = self._get_url('/admin/bucket', params)
        self.assertEqual(len(r['buckets']), len(self.expected_buckets))
        self.assertEqual(r['count'], len(self.expected_buckets))
        self.assertFalse(r['truncated'])
        self.assertNotIn('marker', r)

    def test_bucket_list_marker_only(self) -> None:
        """
        Test with marker only.
        """
        params = self.params.copy()
        sorted_buckets = self.expected_buckets.copy()
        sorted_buckets.sort()
        bucket_key = 100
        params['marker'] = sorted_buckets[bucket_key-1]
        r = self._get_url('/admin/bucket', params)
        self.assertEqual(len(r), len(self.expected_buckets)-bucket_key)

    def test_bucket_list_paginate_until_end(self) -> None:
        """
        Test to paginate through all buckets.
        """
        params = self.params.copy()
        params['max-entries'] = 100

        truncated = True
        last_marker = None
        num_buckets = 0
        buckets = []

        while truncated:
            if last_marker is not None:
                params['marker'] = last_marker

            r = self._get_url('/admin/bucket', params)

            self.assertEqual(len(r['buckets']), params['max-entries'])
            self.assertEqual(r['count'], params['max-entries'])

            buckets += r['buckets']
            num_buckets += r['count']

            if r['truncated']:
                last_marker = r['marker']
            else:
                self.assertNotIn('marker', r)

            truncated = r['truncated']

        self.assertEqual(num_buckets, len(self.expected_buckets))
        self.assertEqual(len(buckets), len(self.expected_buckets))
        sorted_buckets = buckets.copy()
        sorted_buckets.sort()
        sorted_exp_buckets = self.expected_buckets.copy()
        sorted_exp_buckets.sort()
        self.assertEqual(sorted_buckets, sorted_exp_buckets)


if __name__ == '__main__':
    unittest.main()
