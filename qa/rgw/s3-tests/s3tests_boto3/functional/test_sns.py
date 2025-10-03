import json
import pytest
from botocore.exceptions import ClientError
from . import (
    configfile,
    get_iam_root_client,
    get_iam_alt_root_client,
    get_new_bucket_name,
    get_prefix,
    nuke_prefixed_buckets,
)
from .iam import iam_root, iam_alt_root
from .utils import assert_raises, _get_status_and_error_code

def get_new_topic_name():
    return get_new_bucket_name()

def nuke_topics(client, prefix):
    p = client.get_paginator('list_topics')
    for response in p.paginate():
        for topic in response['Topics']:
            arn = topic['TopicArn']
            if prefix not in arn:
                pass
            try:
                client.delete_topic(TopicArn=arn)
            except:
                pass

@pytest.fixture
def sns(iam_root):
    client = get_iam_root_client(service_name='sns')
    yield client
    nuke_topics(client, get_prefix())

@pytest.fixture
def sns_alt(iam_alt_root):
    client = get_iam_alt_root_client(service_name='sns')
    yield client
    nuke_topics(client, get_prefix())

@pytest.fixture
def s3(iam_root):
    # clear region_name to work around Invalid region: region was not a valid DNS name.
    client = get_iam_root_client(service_name='s3', region_name=None)
    yield client
    nuke_prefixed_buckets(get_prefix(), client)

@pytest.fixture
def s3_alt(iam_alt_root):
    # clear region_name to work around Invalid region: region was not a valid DNS name.
    client = get_iam_alt_root_client(service_name='s3', region_name=None)
    yield client
    nuke_prefixed_buckets(get_prefix(), client)


@pytest.mark.iam_account
@pytest.mark.sns
def test_account_topic(sns):
    name = get_new_topic_name()

    response = sns.create_topic(Name=name)
    arn = response['TopicArn']
    assert arn.startswith('arn:aws:sns:')
    assert arn.endswith(f':{name}')

    response = sns.list_topics()
    assert arn in [p['TopicArn'] for p in response['Topics']]

    sns.set_topic_attributes(TopicArn=arn, AttributeName='Policy', AttributeValue='')

    response = sns.get_topic_attributes(TopicArn=arn)
    assert 'Attributes' in response

    sns.delete_topic(TopicArn=arn)

    response = sns.list_topics()
    assert arn not in [p['TopicArn'] for p in response['Topics']]

    with pytest.raises(sns.exceptions.NotFoundException):
        sns.get_topic_attributes(TopicArn=arn)
    sns.delete_topic(TopicArn=arn)

@pytest.mark.iam_account
@pytest.mark.sns
def test_cross_account_topic(sns, sns_alt):
    name = get_new_topic_name()
    arn = sns.create_topic(Name=name)['TopicArn']

    # not authorized to any alt user apis
    with pytest.raises(sns.exceptions.AuthorizationErrorException):
        sns_alt.get_topic_attributes(TopicArn=arn)
    with pytest.raises(sns.exceptions.AuthorizationErrorException):
        sns_alt.set_topic_attributes(TopicArn=arn, AttributeName='Policy', AttributeValue='')
    with pytest.raises(sns.exceptions.AuthorizationErrorException):
        sns_alt.delete_topic(TopicArn=arn)

    # delete returns success
    sns.delete_topic(TopicArn=arn)

    response = sns_alt.list_topics()
    assert arn not in [p['TopicArn'] for p in response['Topics']]

@pytest.mark.iam_account
@pytest.mark.sns
def test_account_topic_publish(sns, s3):
    name = get_new_topic_name()

    response = sns.create_topic(Name=name)
    topic_arn = response['TopicArn']

    bucket = get_new_bucket_name()
    s3.create_bucket(Bucket=bucket)

    config = {'TopicConfigurations': [{
        'Id': 'id',
        'TopicArn': topic_arn,
        'Events': [ 's3:ObjectCreated:*' ],
        }]}
    s3.put_bucket_notification_configuration(
            Bucket=bucket, NotificationConfiguration=config)

@pytest.mark.iam_account
@pytest.mark.iam_cross_account
@pytest.mark.sns
def test_cross_account_topic_publish(sns, s3_alt, iam_alt_root):
    name = get_new_topic_name()

    response = sns.create_topic(Name=name)
    topic_arn = response['TopicArn']

    bucket = get_new_bucket_name()
    s3_alt.create_bucket(Bucket=bucket)

    config = {'TopicConfigurations': [{
        'Id': 'id',
        'TopicArn': topic_arn,
        'Events': [ 's3:ObjectCreated:*' ],
        }]}

    # expect AccessDenies because no resource policy allows cross-account access
    e = assert_raises(ClientError, s3_alt.put_bucket_notification_configuration,
                      Bucket=bucket, NotificationConfiguration=config)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    # add topic policy to allow the alt user
    alt_principal = iam_alt_root.get_user()['User']['Arn']
    policy = json.dumps({
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'AWS': alt_principal},
            'Action': 'sns:Publish',
            'Resource': topic_arn
            }]
        })
    sns.set_topic_attributes(TopicArn=topic_arn, AttributeName='Policy',
                             AttributeValue=policy)

    s3_alt.put_bucket_notification_configuration(
            Bucket=bucket, NotificationConfiguration=config)
