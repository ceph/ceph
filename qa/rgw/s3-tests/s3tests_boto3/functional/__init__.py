import pytest
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.handlers import disable_signing
import configparser
import datetime
import time
import os
import munch
import random
import string
import itertools
import urllib3
import re

config = munch.Munch

# this will be assigned by setup()
prefix = None

def get_prefix():
    assert prefix is not None
    return prefix

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )

def get_buckets_list(client=None, prefix=None):
    if client == None:
        client = get_client()
    if prefix == None:
        prefix = get_prefix()
    response = client.list_buckets()
    bucket_dicts = response['Buckets']
    buckets_list = []
    for bucket in bucket_dicts:
        if prefix in bucket['Name']:
            buckets_list.append(bucket['Name'])

    return buckets_list

def get_objects_list(bucket, client=None, prefix=None):
    if client == None:
        client = get_client()

    if prefix == None:
        response = client.list_objects(Bucket=bucket)
    else:
        response = client.list_objects(Bucket=bucket, Prefix=prefix)
    objects_list = []

    if 'Contents' in response:
        contents = response['Contents']
        for obj in contents:
            objects_list.append(obj['Key'])

    return objects_list

# generator function that returns object listings in batches, where each
# batch is a list of dicts compatible with delete_objects()
def list_versions(client, bucket, batch_size):
    kwargs = {'Bucket': bucket, 'MaxKeys': batch_size}
    truncated = True
    while truncated:
        listing = client.list_object_versions(**kwargs)

        kwargs['KeyMarker'] = listing.get('NextKeyMarker')
        kwargs['VersionIdMarker'] = listing.get('NextVersionIdMarker')
        truncated = listing['IsTruncated']

        objs = listing.get('Versions', []) + listing.get('DeleteMarkers', [])
        if len(objs):
            yield [{'Key': o['Key'], 'VersionId': o['VersionId']} for o in objs]

def nuke_bucket(client, bucket):
    batch_size = 128
    max_retain_date = None

    # list and delete objects in batches
    for objects in list_versions(client, bucket, batch_size):
        delete = client.delete_objects(Bucket=bucket,
                Delete={'Objects': objects, 'Quiet': True},
                BypassGovernanceRetention=True)

        # check for object locks on 403 AccessDenied errors
        for err in delete.get('Errors', []):
            if err.get('Code') != 'AccessDenied':
                continue
            try:
                res = client.get_object_retention(Bucket=bucket,
                        Key=err['Key'], VersionId=err['VersionId'])
                retain_date = res['Retention']['RetainUntilDate']
                if not max_retain_date or max_retain_date < retain_date:
                    max_retain_date = retain_date
            except ClientError:
                pass

    if max_retain_date:
        # wait out the retention period (up to 60 seconds)
        now = datetime.datetime.now(max_retain_date.tzinfo)
        if max_retain_date > now:
            delta = max_retain_date - now
            if delta.total_seconds() > 60:
                raise RuntimeError('bucket {} still has objects \
locked for {} more seconds, not waiting for \
bucket cleanup'.format(bucket, delta.total_seconds()))
            print('nuke_bucket', bucket, 'waiting', delta.total_seconds(),
                    'seconds for object locks to expire')
            time.sleep(delta.total_seconds())

        for objects in list_versions(client, bucket, batch_size):
            client.delete_objects(Bucket=bucket,
                    Delete={'Objects': objects, 'Quiet': True},
                    BypassGovernanceRetention=True)

    client.delete_bucket(Bucket=bucket)

def nuke_prefixed_buckets(prefix, client=None):
    if client == None:
        client = get_client()

    buckets = get_buckets_list(client, prefix)

    err = None
    for bucket_name in buckets:
        try:
            nuke_bucket(client, bucket_name)
        except Exception as e:
            # The exception shouldn't be raised when doing cleanup. Pass and continue
            # the bucket cleanup process. Otherwise left buckets wouldn't be cleared
            # resulting in some kind of resource leak. err is used to hint user some
            # exception once occurred.
            err = e
            pass
    if err:
        raise err

    print('Done with cleanup of buckets in tests.')

def configured_storage_classes():
    sc = ['STANDARD']

    extra_sc = re.split(r"[\b\W\b]+", config.storage_classes)

    for item in extra_sc:
        if item != 'STANDARD':
             sc.append(item)

    sc = [i for i in sc if i]
    print("storage classes configured: " + str(sc))

    return sc

def configure():
    cfg = configparser.RawConfigParser()
    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    cfg.read(path)

    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')
    if not cfg.has_section("s3 main"):
        raise RuntimeError('Your config file is missing the "s3 main" section!')
    if not cfg.has_section("s3 alt"):
        raise RuntimeError('Your config file is missing the "s3 alt" section!')
    if not cfg.has_section("s3 tenant"):
        raise RuntimeError('Your config file is missing the "s3 tenant" section!')

    global prefix

    defaults = cfg.defaults()

    # vars from the DEFAULT section
    config.default_host = defaults.get("host")
    config.default_port = int(defaults.get("port"))
    config.default_is_secure = cfg.getboolean('DEFAULT', "is_secure")

    proto = 'https' if config.default_is_secure else 'http'
    config.default_endpoint = "%s://%s:%d" % (proto, config.default_host, config.default_port)

    try:
        config.default_ssl_verify = cfg.getboolean('DEFAULT', "ssl_verify")
    except configparser.NoOptionError:
        config.default_ssl_verify = False

    # Disable InsecureRequestWarning reported by urllib3 when ssl_verify is False
    if not config.default_ssl_verify:
        urllib3.disable_warnings()

    # vars from the main section
    config.main_access_key = cfg.get('s3 main',"access_key")
    config.main_secret_key = cfg.get('s3 main',"secret_key")
    config.main_display_name = cfg.get('s3 main',"display_name")
    config.main_user_id = cfg.get('s3 main',"user_id")
    config.main_email = cfg.get('s3 main',"email")
    try:
        config.main_kms_keyid = cfg.get('s3 main',"kms_keyid")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.main_kms_keyid = 'testkey-1'

    try:
        config.main_kms_keyid2 = cfg.get('s3 main',"kms_keyid2")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.main_kms_keyid2 = 'testkey-2'

    try:
        config.main_api_name = cfg.get('s3 main',"api_name")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.main_api_name = ""
        pass

    try:
        config.storage_classes = cfg.get('s3 main',"storage_classes")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.storage_classes = ""
        pass

    try:
        config.lc_debug_interval = int(cfg.get('s3 main',"lc_debug_interval"))
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.lc_debug_interval = 10

    try:
        config.rgw_restore_debug_interval = int(cfg.get('s3 main',"rgw_restore_debug_interval"))
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.rgw_restore_debug_interval = 100

    try:
        config.rgw_restore_processor_period = int(cfg.get('s3 main',"rgw_restore_processor_period"))
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.rgw_restore_processor_period = 100

    config.alt_access_key = cfg.get('s3 alt',"access_key")
    config.alt_secret_key = cfg.get('s3 alt',"secret_key")
    config.alt_display_name = cfg.get('s3 alt',"display_name")
    config.alt_user_id = cfg.get('s3 alt',"user_id")
    config.alt_email = cfg.get('s3 alt',"email")

    config.tenant_access_key = cfg.get('s3 tenant',"access_key")
    config.tenant_secret_key = cfg.get('s3 tenant',"secret_key")
    config.tenant_display_name = cfg.get('s3 tenant',"display_name")
    config.tenant_user_id = cfg.get('s3 tenant',"user_id")
    config.tenant_email = cfg.get('s3 tenant',"email")
    config.tenant_name = cfg.get('s3 tenant',"tenant")

    config.iam_access_key = cfg.get('iam',"access_key")
    config.iam_secret_key = cfg.get('iam',"secret_key")
    config.iam_display_name = cfg.get('iam',"display_name")
    config.iam_user_id = cfg.get('iam',"user_id")
    config.iam_email = cfg.get('iam',"email")

    config.iam_root_access_key = cfg.get('iam root',"access_key")
    config.iam_root_secret_key = cfg.get('iam root',"secret_key")
    config.iam_root_user_id = cfg.get('iam root',"user_id")
    config.iam_root_email = cfg.get('iam root',"email")

    config.iam_alt_root_access_key = cfg.get('iam alt root',"access_key")
    config.iam_alt_root_secret_key = cfg.get('iam alt root',"secret_key")
    config.iam_alt_root_user_id = cfg.get('iam alt root',"user_id")
    config.iam_alt_root_email = cfg.get('iam alt root',"email")

    # vars from the fixtures section
    template = cfg.get('fixtures', "bucket prefix", fallback='test-{random}-')
    prefix = choose_bucket_prefix(template=template)
    template = cfg.get('fixtures', "iam name prefix", fallback="s3-tests-")
    config.iam_name_prefix = choose_bucket_prefix(template=template)
    template = cfg.get('fixtures', "iam path prefix", fallback="/s3-tests/")
    config.iam_path_prefix = choose_bucket_prefix(template=template)

    if cfg.has_section("s3 cloud"):
        get_cloud_config(cfg)
    else:
        config.cloud_storage_class = None

def setup():
    alt_client = get_alt_client()
    tenant_client = get_tenant_client()
    nuke_prefixed_buckets(prefix=prefix)
    nuke_prefixed_buckets(prefix=prefix, client=alt_client)
    nuke_prefixed_buckets(prefix=prefix, client=tenant_client)

def teardown():
    alt_client = get_alt_client()
    tenant_client = get_tenant_client()
    nuke_prefixed_buckets(prefix=prefix)
    nuke_prefixed_buckets(prefix=prefix, client=alt_client)
    nuke_prefixed_buckets(prefix=prefix, client=tenant_client)
    try:
        iam_client = get_iam_client()
        list_roles_resp = iam_client.list_roles()
        for role in list_roles_resp['Roles']:
            list_policies_resp = iam_client.list_role_policies(RoleName=role['RoleName'])
            for policy in list_policies_resp['PolicyNames']:
                del_policy_resp = iam_client.delete_role_policy(
                                         RoleName=role['RoleName'],
                                         PolicyName=policy
                                        )
            del_role_resp = iam_client.delete_role(RoleName=role['RoleName'])
        list_oidc_resp = iam_client.list_open_id_connect_providers()
        for oidcprovider in list_oidc_resp['OpenIDConnectProviderList']:
            del_oidc_resp = iam_client.delete_open_id_connect_provider(
                        OpenIDConnectProviderArn=oidcprovider['Arn']
                    )
    except:
        pass

@pytest.fixture(scope="package")
def configfile():
    configure()
    return config

@pytest.fixture(autouse=True)
def setup_teardown(configfile):
    setup()
    yield
    teardown()

def check_webidentity():
    cfg = configparser.RawConfigParser()
    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    cfg.read(path)
    if not cfg.has_section("webidentity"):
        raise RuntimeError('Your config file is missing the "webidentity" section!')

    config.webidentity_thumbprint = cfg.get('webidentity', "thumbprint")
    config.webidentity_aud = cfg.get('webidentity', "aud")
    config.webidentity_token = cfg.get('webidentity', "token")
    config.webidentity_realm = cfg.get('webidentity', "KC_REALM")
    config.webidentity_sub = cfg.get('webidentity', "sub")
    config.webidentity_azp = cfg.get('webidentity', "azp")
    config.webidentity_user_token = cfg.get('webidentity', "user_token")

def get_cloud_config(cfg):
    config.cloud_host = cfg.get('s3 cloud',"host")
    config.cloud_port = int(cfg.get('s3 cloud',"port"))
    config.cloud_is_secure = cfg.getboolean('s3 cloud', "is_secure")

    proto = 'https' if config.cloud_is_secure else 'http'
    config.cloud_endpoint = "%s://%s:%d" % (proto, config.cloud_host, config.cloud_port)

    config.cloud_access_key = cfg.get('s3 cloud',"access_key")
    config.cloud_secret_key = cfg.get('s3 cloud',"secret_key")

    try:
        config.cloud_storage_class = cfg.get('s3 cloud', "cloud_storage_class")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.cloud_storage_class = None
    
    try:
        config.cloud_retain_head_object = cfg.get('s3 cloud',"retain_head_object")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.cloud_retain_head_object = None

    try:
        config.allow_read_through = cfg.get('s3 cloud',"allow_read_through")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.allow_read_through = False

    try:
        config.cloud_target_path = cfg.get('s3 cloud',"target_path")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.cloud_target_path = None

    try:
        config.cloud_target_storage_class = cfg.get('s3 cloud',"target_storage_class")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.cloud_target_storage_class = 'STANDARD'

    try:
        config.cloud_regular_storage_class = cfg.get('s3 cloud', "storage_class")
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.cloud_regular_storage_class  = None

    try:
        config.read_through_restore_days = int(cfg.get('s3 cloud', "read_through_restore_days"))
    except (configparser.NoSectionError, configparser.NoOptionError):
        config.read_through_restore_days = 10


def get_client(client_config=None):
    if client_config == None:
        client_config = Config(signature_version='s3v4')

    client = boto3.client(service_name='s3',
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=client_config)
    return client

def get_v2_client():
    client = boto3.client(service_name='s3',
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=Config(signature_version='s3'))
    return client

def get_sts_client(**kwargs):
    kwargs.setdefault('region_name', '')
    kwargs.setdefault('aws_access_key_id', config.alt_access_key)
    kwargs.setdefault('aws_secret_access_key', config.alt_secret_key)
    kwargs.setdefault('config', Config(signature_version='s3v4'))

    client = boto3.client(service_name='sts',
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          **kwargs)
    return client

def get_iam_client(**kwargs):
    kwargs.setdefault('region_name', '')
    kwargs.setdefault('aws_access_key_id', config.iam_access_key)
    kwargs.setdefault('aws_secret_access_key', config.iam_secret_key)

    client = boto3.client(service_name='iam',
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        **kwargs)
    return client

def get_iam_s3client(**kwargs):
    kwargs.setdefault('aws_access_key_id', config.iam_access_key)
    kwargs.setdefault('aws_secret_access_key', config.iam_secret_key)
    kwargs.setdefault('config', Config(signature_version='s3v4'))

    client = boto3.client(service_name='s3',
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          **kwargs)
    return client

def get_iam_root_s3client(**kwargs):
    kwargs.setdefault('aws_access_key_id', config.iam_root_access_key)
    kwargs.setdefault('aws_secret_access_key', config.iam_root_secret_key)
    kwargs.setdefault('config', Config(signature_version='s3v4'))

    client = boto3.client(service_name='s3',
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          **kwargs)
    return client

def get_iam_root_client(**kwargs):
    kwargs.setdefault('service_name', 'iam')
    kwargs.setdefault('region_name', '')
    kwargs.setdefault('aws_access_key_id', config.iam_root_access_key)
    kwargs.setdefault('aws_secret_access_key', config.iam_root_secret_key)

    return boto3.client(endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        **kwargs)

def get_iam_alt_root_client(**kwargs):
    kwargs.setdefault('service_name', 'iam')
    kwargs.setdefault('region_name', '')
    kwargs.setdefault('aws_access_key_id', config.iam_alt_root_access_key)
    kwargs.setdefault('aws_secret_access_key', config.iam_alt_root_secret_key)

    return boto3.client(endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        **kwargs)

def get_alt_client(client_config=None):
    if client_config == None:
        client_config = Config(signature_version='s3v4')

    client = boto3.client(service_name='s3',
                        aws_access_key_id=config.alt_access_key,
                        aws_secret_access_key=config.alt_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=client_config)
    return client

def get_cloud_client(client_config=None):
    if client_config == None:
        client_config = Config(signature_version='s3v4')

    client = boto3.client(service_name='s3',
                        aws_access_key_id=config.cloud_access_key,
                        aws_secret_access_key=config.cloud_secret_key,
                        endpoint_url=config.cloud_endpoint,
                        use_ssl=config.cloud_is_secure,
                        config=client_config)
    return client

def get_tenant_client(client_config=None):
    if client_config == None:
        client_config = Config(signature_version='s3v4')

    client = boto3.client(service_name='s3',
                        aws_access_key_id=config.tenant_access_key,
                        aws_secret_access_key=config.tenant_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=client_config)
    return client

def get_v2_tenant_client():
    client_config = Config(signature_version='s3')
    client = boto3.client(service_name='s3',
                          aws_access_key_id=config.tenant_access_key,
                          aws_secret_access_key=config.tenant_secret_key,
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=client_config)
    return client

def get_tenant_iam_client():

    client = boto3.client(service_name='iam',
                          region_name='us-east-1',
                          aws_access_key_id=config.tenant_access_key,
                          aws_secret_access_key=config.tenant_secret_key,
                          endpoint_url=config.default_endpoint,
                          verify=config.default_ssl_verify,
                          use_ssl=config.default_is_secure)
    return client

def get_alt_iam_client():

    client = boto3.client(service_name='iam',
                          region_name='',
                          aws_access_key_id=config.alt_access_key,
                          aws_secret_access_key=config.alt_secret_key,
                          endpoint_url=config.default_endpoint,
                          verify=config.default_ssl_verify,
                          use_ssl=config.default_is_secure)
    return client

def get_unauthenticated_client():
    client = boto3.client(service_name='s3',
                        aws_access_key_id='',
                        aws_secret_access_key='',
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=Config(signature_version=UNSIGNED))
    return client

def get_bad_auth_client(aws_access_key_id='badauth'):
    client = boto3.client(service_name='s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key='roflmao',
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=Config(signature_version='s3v4'))
    return client

def get_svc_client(client_config=None, svc='s3'):
    if client_config == None:
        client_config = Config(signature_version='s3v4')

    client = boto3.client(service_name=svc,
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify,
                        config=client_config)
    return client

bucket_counter = itertools.count(1)

def get_new_bucket_name():
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    return name

def get_new_bucket_resource(name=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    s3 = boto3.resource('s3',
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify)
    if name is None:
        name = get_new_bucket_name()
    bucket = s3.Bucket(name)
    bucket_location = bucket.create()
    return bucket

def get_new_bucket(client=None, name=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if client is None:
        client = get_client()
    if name is None:
        name = get_new_bucket_name()

    client.create_bucket(Bucket=name)
    return name

def get_parameter_name():
    parameter_name=""
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )
    while rand:
        parameter_name = '{random}'.format(random=rand)
        if len(parameter_name) <= 10:
            return parameter_name
        rand = rand[:-1]
    return parameter_name

def get_sts_user_id():
    return config.alt_user_id

def get_config_is_secure():
    return config.default_is_secure

def get_config_host():
    return config.default_host

def get_config_port():
    return config.default_port

def get_config_endpoint():
    return config.default_endpoint

def get_config_ssl_verify():
    return config.default_ssl_verify

def get_main_aws_access_key():
    return config.main_access_key

def get_main_aws_secret_key():
    return config.main_secret_key

def get_main_display_name():
    return config.main_display_name

def get_main_user_id():
    return config.main_user_id

def get_main_email():
    return config.main_email

def get_main_api_name():
    return config.main_api_name

def get_main_kms_keyid():
    return config.main_kms_keyid

def get_secondary_kms_keyid():
    return config.main_kms_keyid2

def get_alt_aws_access_key():
    return config.alt_access_key

def get_alt_aws_secret_key():
    return config.alt_secret_key

def get_alt_display_name():
    return config.alt_display_name

def get_alt_user_id():
    return config.alt_user_id

def get_alt_email():
    return config.alt_email

def get_tenant_aws_access_key():
    return config.tenant_access_key

def get_tenant_aws_secret_key():
    return config.tenant_secret_key

def get_tenant_display_name():
    return config.tenant_display_name

def get_tenant_name():
    return config.tenant_name

def get_tenant_user_id():
    return config.tenant_user_id

def get_tenant_email():
    return config.tenant_email

def get_thumbprint():
    return config.webidentity_thumbprint

def get_aud():
    return config.webidentity_aud

def get_sub():
    return config.webidentity_sub

def get_azp():
    return config.webidentity_azp

def get_token():
    return config.webidentity_token

def get_realm_name():
    return config.webidentity_realm

def get_iam_name_prefix():
    return config.iam_name_prefix

def make_iam_name(name):
    return config.iam_name_prefix + name

def get_iam_path_prefix():
    return config.iam_path_prefix

def get_iam_access_key():
    return config.iam_access_key

def get_iam_secret_key():
    return config.iam_secret_key

def get_iam_root_user_id():
    return config.iam_root_user_id

def get_iam_root_email():
    return config.iam_root_email

def get_iam_alt_root_user_id():
    return config.iam_alt_root_user_id

def get_iam_alt_root_email():
    return config.iam_alt_root_email

def get_user_token():
    return config.webidentity_user_token

def get_cloud_storage_class():
    return config.cloud_storage_class

def get_cloud_retain_head_object():
    return config.cloud_retain_head_object

def get_allow_read_through():
    return config.allow_read_through

def get_cloud_regular_storage_class():
    return config.cloud_regular_storage_class

def get_cloud_target_path():
    return config.cloud_target_path

def get_cloud_target_storage_class():
    return config.cloud_target_storage_class

def get_lc_debug_interval():
    return config.lc_debug_interval

def get_restore_debug_interval():
    return config.rgw_restore_debug_interval

def get_restore_processor_period():
    return config.rgw_restore_processor_period

def get_read_through_days():
    return config.read_through_restore_days

def create_iam_user_s3client(client):
    prefix = get_iam_path_prefix()

    # generate random name
    randname = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(8)
    )
    name = make_iam_name(randname)

    user = client.create_user(UserName=name, Path=prefix)

    # create s3 access and secret keys
    keys = client.create_access_key(UserName=user['User']['UserName'])

    # create s3 client
    return get_iam_s3client(
        aws_access_key_id=keys['AccessKey']['AccessKeyId'],
        aws_secret_access_key=keys['AccessKey']['SecretAccessKey'],
    )
