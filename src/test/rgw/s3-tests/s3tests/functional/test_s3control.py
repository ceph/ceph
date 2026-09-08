import boto3
from botocore.exceptions import ClientError
import json
import pytest

from . import (
    configfile,
    setup_teardown,
    get_iam_root_client,
    get_iam_root_account_id,
    get_new_bucket_name,
    )
from .utils import (
    assert_raises,
    _get_status_and_error_code,
    )

@pytest.mark.s3control
def test_account_public_access_block():
    s3control = get_iam_root_client(service_name='s3control', region_name='us-east-1')
    account_id = get_iam_root_account_id()

    # delete default configuration if it exists
    response = s3control.delete_public_access_block(AccountId=account_id)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    # re-delete should still return 204
    response = s3control.delete_public_access_block(AccountId=account_id)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    # get returns 404
    e = assert_raises(ClientError, s3control.get_public_access_block, AccountId=account_id)
    assert (404, 'NoSuchPublicAccessBlockConfiguration') == _get_status_and_error_code(e.response)

    s3control.put_public_access_block(
            AccountId=account_id,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': False,
                'BlockPublicPolicy': False,
                'RestrictPublicBuckets': False
            })
    try:
        response = s3control.get_public_access_block(AccountId=account_id)
        assert response['PublicAccessBlockConfiguration']['BlockPublicAcls']
        assert not response['PublicAccessBlockConfiguration']['IgnorePublicAcls']
        assert not response['PublicAccessBlockConfiguration']['BlockPublicPolicy']
        assert not response['PublicAccessBlockConfiguration']['RestrictPublicBuckets']

        s3 = get_iam_root_client(service_name='s3')
        bucket = get_new_bucket_name()

        # reject CreateBucket with public acls
        e = assert_raises(ClientError, s3.create_bucket, Bucket=bucket, ACL='public-read')
        assert (403, 'AccessDenied') == _get_status_and_error_code(e.response)

        s3.create_bucket(Bucket=bucket)
        try:
            # reject PutBucketAcl with public acls
            e = assert_raises(ClientError, s3.put_bucket_acl, Bucket=bucket, ACL='public-read')
            assert (403, 'AccessDenied') == _get_status_and_error_code(e.response)

            # test interaction with bucket-level configuration
            s3.put_public_access_block(
                    Bucket=bucket,
                    PublicAccessBlockConfiguration={
                        'BlockPublicAcls': False,
                        'IgnorePublicAcls': False,
                        'BlockPublicPolicy': True,
                        'RestrictPublicBuckets': False
                    })
            public_policy = json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "*",
                    "Resource": [
                        f"arn:aws:s3:::{bucket}",
                        f"arn:aws:s3:::{bucket}/*"
                    ]
                }]
            })
            # reject PutBucketPolicy with public policy based on bucket config
            e = assert_raises(ClientError, s3.put_bucket_policy,
                              Bucket=bucket, Policy=public_policy)
            assert (403, 'AccessDenied') == _get_status_and_error_code(e.response)
        finally:
            s3.delete_bucket(Bucket=bucket)
    finally:
        s3control.delete_public_access_block(AccountId=account_id)
