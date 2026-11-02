#!/usr/bin/env python3
"""Exercise Swift FormPost with the default Swift URL settings.

Requires a running RGW with S3 and Swift enabled, radosgw-admin in PATH,
and rgw_swift_account_in_url=false. Run through run-swift-formpost.sh,
setting RGW_HTTP_ENDPOINT_URL when running outside teuthology.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import subprocess
import time
import unittest
import uuid

import boto3
import requests
from botocore.config import Config


class SwiftFormPostPrincipalTypeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        uid = 'formpost-principal-' + uuid.uuid4().hex
        access_key = secrets.token_hex(10)
        secret_key = secrets.token_hex(20)
        cls.temp_url_key = secrets.token_hex(20)
        subprocess.run([
            'radosgw-admin', 'user', 'create', '--uid', uid,
            '--display-name', 'Swift FormPost principal type test',
            '--access-key', access_key, '--secret-key', secret_key,
            '--temp-url-key', 'unused-formpost-key',
            '--temp-url-key-2', cls.temp_url_key,
        ], check=True, stdout=subprocess.DEVNULL)
        cls.addClassCleanup(subprocess.run, [
            'radosgw-admin', 'user', 'rm', '--uid', uid, '--purge-data',
        ], check=True, stdout=subprocess.DEVNULL)
        cls.client = boto3.client(
            's3', endpoint_url=os.environ['RGW_HTTP_ENDPOINT_URL'],
            aws_access_key_id=access_key, aws_secret_access_key=secret_key,
            region_name='us-east-1', verify=False,
            config=Config(s3={'addressing_style': 'path'},
                          retries={'total_max_attempts': 1}),
        )
        cls.endpoint = cls.client.meta.endpoint_url.rstrip('/')

    def setUp(self):
        self.bucket = 'formpost-principal-' + uuid.uuid4().hex
        self.client.create_bucket(Bucket=self.bucket)
        self.addCleanup(subprocess.run, [
            'radosgw-admin', 'bucket', 'rm', '--bucket', self.bucket,
            '--purge-objects',
        ], check=True, stdout=subprocess.DEVNULL)
        self.path = '/swift/v1/' + self.bucket + '/'
        self.key = 'test.txt'
        self.set_policy()

    def set_policy(self, condition=None):
        statements = [{
            'Effect': 'Allow', 'Principal': '*',
            'Action': ['s3:GetObject', 's3:PutObject'],
            'Resource': f'arn:aws:s3:::{self.bucket}/*',
        }]
        if condition is not None:
            statements.append({
                'Effect': 'Deny', 'Principal': '*', 'Action': 's3:PutObject',
                'Resource': f'arn:aws:s3:::{self.bucket}/*',
                'Condition': condition,
            })
        self.client.put_bucket_policy(Bucket=self.bucket, Policy=json.dumps({
            'Version': '2012-10-17', 'Statement': statements,
        }))

    def post(self, body, digest, encoding, *, valid_signature=True,
             expired=False):
        fields = {
            'redirect': '',
            'max_file_size': '1024',
            'max_file_count': '1',
            'expires': '1' if expired else str(int(time.time()) + 3600),
        }
        message = '\n'.join([self.path, *fields.values()]).encode()
        signature = hmac.new(
            self.temp_url_key.encode(), message, getattr(hashlib, digest)
        ).digest()
        if not valid_signature:
            signature = bytes([signature[0] ^ 1]) + signature[1:]
        if encoding == 'hex':
            fields['signature'] = signature.hex()
        else:
            fields['signature'] = digest + ':' + base64.urlsafe_b64encode(
                signature).decode()

        # No X-Auth-Token: the request starts anonymous and authenticates
        # using the form signature. Use the second temp URL key as well.
        return requests.post(
            self.endpoint + self.path, data=fields,
            files={'file': (self.key, body)}, verify=False,
            allow_redirects=False, timeout=30,
        )

    def assert_body(self, expected):
        with self.client.get_object(
                Bucket=self.bucket, Key=self.key)['Body'] as body:
            self.assertEqual(body.read(), expected)

    def test_conditional_denies(self):
        cases = [
            ('StringEquals', 'User', 403),
            ('StringEquals', 'Anonymous', 200),
            ('StringNotEquals', 'User', 200),
            ('StringNotEquals', 'Anonymous', 403),
        ]
        for digest in ('sha1', 'sha256', 'sha512'):
            for encoding in ('hex', 'base64'):
                for operator, principal_type, expected_status in cases:
                    with self.subTest(digest=digest, encoding=encoding,
                                      operator=operator, type=principal_type):
                        self.set_policy()
                        original = b'before conditional deny'
                        response = self.post(original, digest, encoding)
                        self.assertEqual(response.status_code, 200,
                                         response.text)
                        self.assert_body(original)

                        self.set_policy({
                            operator: {'aws:PrincipalType': principal_type},
                        })
                        updated = b'after conditional deny'
                        response = self.post(updated, digest, encoding)
                        self.assertEqual(response.status_code, expected_status,
                                         response.text)
                        self.assert_body(
                            original if expected_status == 403 else updated)

    def test_invalid_or_expired_signature(self):
        original = b'preserve existing object'
        for digest in ('sha1', 'sha256', 'sha512'):
            for encoding in ('hex', 'base64'):
                response = self.post(original, digest, encoding)
                self.assertEqual(response.status_code, 200, response.text)
                for options, error in (
                        ({'valid_signature': False},
                         'FormPost: Invalid Signature'),
                        ({'expired': True}, 'FormPost: Form Expired')):
                    with self.subTest(digest=digest, encoding=encoding,
                                      options=options):
                        response = self.post(
                            b'rejected upload', digest, encoding, **options)
                        self.assertEqual(response.status_code, 401,
                                         response.text)
                        self.assertIn(error, response.text)
                        self.assert_body(original)


if __name__ == '__main__':
    unittest.main()
