=========
STS Lite
=========

Ceph Object Gateway provides support for a subset of Amazon Secure Token Service
(STS) APIs. STS Lite provides access to a set of temporary credentials for
Identity and Access Management.

STS authentication mechanism has been integrated with Keystone in Ceph Object
Gateway. A set of temporary security credentials is returned after authenticating
a set of AWS credentials with Keystone. These temporary credentials can be used
to make subsequent S3 calls which will be authenticated by the STS engine,
resulting in less load on the Keystone server.

STS Lite REST APIs
==================

The following STS Lite REST APIs have been implemented in Ceph Object Gateway:

1. GetSessionToken: Returns a set of temporary credentials for a set of AWS
credentials. This API can be used for initial authentication with Keystone 
and the temporary credentials returned can be used to make subsequent S3
calls. The temporary credentials will have the same permission as that of the 
AWS credentials.

Parameters:
    **DurationSeconds** (Integer/ Optional): The duration in seconds for which the
    credentials should remain valid. Its default value is 3600. Its default max
    value is 43200 which is can be configured using rgw sts max session duration.

    **SerialNumber** (String/ Optional): The Id number of the MFA device associated 
    with the user making the GetSessionToken call.

    **TokenCode** (String/ Optional): The value provided by the MFA device, if MFA is required.

An end user needs to attach a policy to allow invocation of GetSessionToken API using its permanent
credentials and to allow subsequent s3 operations invocation using only the temporary credentials returned
by GetSessionToken.
The following is an example of attaching the policy to a user 'TESTER1'::

    s3curl.pl --debug --id admin -- -s -v -X POST "http://localhost:8000/?Action=PutUserPolicy&PolicyName=Policy1&UserName=TESTER1&PolicyDocument=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Deny\",\"Action\":\"s3:*\",\"Resource\":\[\"*\"\],\"Condition\":\{\"BoolIfExists\":\{\"sts:authentication\":\"false\"\}\}\},\{\"Effect\":\"Allow\",\"Action\":\"sts:GetSessionToken\",\"Resource\":\"*\",\"Condition\":\{\"BoolIfExists\":\{\"sts:authentication\":\"false\"\}\}\}\]\}&Version=2010-05-08"

The user attaching the policy needs to have admin caps. For example::

    radosgw-admin caps add --uid="TESTER" --caps="user-policy=*"

2. AssumeRole: Returns a set of temporary credentials that can be used for 
cross-account access. The temporary credentials will have permissions that are
allowed by both - permission policies attached with the Role and policy attached
with the AssumeRole API.

Parameters:
    **RoleArn** (String/ Required): ARN of the Role to Assume.

    **RoleSessionName** (String/ Required): An Identifier for the assumed role
    session.

    **Policy** (String/ Optional): An IAM Policy in JSON format.

    **DurationSeconds** (Integer/ Optional): The duration in seconds of the session.
    Its default value is 3600.

    **ExternalId** (String/ Optional): A unique Id that might be used when a role is
    assumed in another account.

    **SerialNumber** (String/ Optional): The Id number of the MFA device associated
    with the user making the AssumeRole call.

    **TokenCode** (String/ Optional): The value provided by the MFA device, if the
    trust policy of the role being assumed requires MFA.


STS Lite Configuration
======================

The following configurable options are available for STS Lite integration::

  [client.radosgw.gateway]
  rgw sts key = {sts key for encrypting the session token}
  rgw s3 auth use sts = true

The above STS configurables can be used with the Keystone configurables if one
needs to use STS Lite in conjunction with Keystone. The complete set of
configurable options will be::

  [client.radosgw.gateway]
  rgw sts key = {sts key for encrypting/ decrypting the session token}
  rgw s3 auth use sts = true

  rgw keystone url = {keystone server url:keystone server admin port}
  rgw keystone admin project = {keystone admin project name}
  rgw keystone admin tenant = {keystone service tenant name}
  rgw keystone admin domain = {keystone admin domain name}
  rgw keystone api version = {keystone api version}
  rgw keystone implicit tenants = {true for private tenant for each new user}
  rgw keystone admin password = {keystone service tenant user name}
  rgw keystone admin user = keystone service tenant user password}
  rgw keystone accepted roles = {accepted user roles}
  rgw keystone token cache size = {number of tokens to cache}
  rgw keystone revocation interval = {number of seconds before checking revoked tickets}
  rgw s3 auth use keystone = true
  rgw nss db path = {path to nss db}

Note: By default, STS and S3 APIs co-exist in the same namespace, and both S3
and STS APIs can be accessed via the same endpoint in Ceph Object Gateway.

Example showing how to Use STS Lite with Keystone
=================================================

The following are the steps needed to use STS Lite with Keystone. Boto 3.x has
been used to write an example code to show the integration of STS Lite with
Keystone.

1. Generate EC2 credentials :

.. code-block:: javascript

  openstack ec2 credentials create
  +------------+--------------------------------------------------------+
  | Field      | Value                                                  |
  +------------+--------------------------------------------------------+
  | access     | b924dfc87d454d15896691182fdeb0ef                       |
  | links      | {u'self': u'http://192.168.0.15/identity/v3/users/     |
  |            | 40a7140e424f493d8165abc652dc731c/credentials/          |
  |            | OS-EC2/b924dfc87d454d15896691182fdeb0ef'}              |
  | project_id | c703801dccaf4a0aaa39bec8c481e25a                       |
  | secret     | 6a2142613c504c42a94ba2b82147dc28                       |
  | trust_id   | None                                                   |
  | user_id    | 40a7140e424f493d8165abc652dc731c                       |
  +------------+--------------------------------------------------------+

2. Use the credentials created in the step 1. to get back a set of temporary
   credentials using GetSessionToken API.

.. code-block:: python

    import boto3
 
    access_key = <ec2 access key>
    secret_key = <ec2 secret key>

    client = boto3.client('sts',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=<STS URL>,
    region_name='',
    )

    response = client.get_session_token(
        DurationSeconds=43200
    )

3. The temporary credentials obtained in step 2. can be used for making S3 calls:

.. code-block:: python

    s3client = boto3.client('s3',
      aws_access_key_id = response['Credentials']['AccessKeyId'],
      aws_secret_access_key = response['Credentials']['SecretAccessKey'],
      aws_session_token = response['Credentials']['SessionToken'],
      endpoint_url=<S3 URL>,
      region_name='')

    bucket = s3client.create_bucket(Bucket='my-new-shiny-bucket')
    response = s3client.list_buckets()
    for bucket in response["Buckets"]:
        print "{name}\t{created}".format(
                    name = bucket['Name'],
                    created = bucket['CreationDate'],
    )

4. The following is an example of AssumeRole API call:

.. code-block:: python

    import boto3

    access_key = <ec2 access key>
    secret_key = <ec2 secret key>

    client = boto3.client('sts',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=<STS URL>,
    region_name='',
    )

    response = client.assume_role(
    RoleArn='arn:aws:iam:::role/application_abc/component_xyz/S3Access',
    RoleSessionName='Bob',
    DurationSeconds=3600
    )


Note: A role 'S3Access', needs to be created before calling the AssumeRole API.

Limitations and Workarounds
===========================

1. Keystone currently supports only S3 requests, hence in order to successfully 
authenticate an STS request, the following workaround needs to be added to boto
to the following file - botocore/auth.py

Lines 13-16 have been added as a workaround in the code block below:

.. code-block:: python

  class SigV4Auth(BaseSigner):
    """
    Sign a request with Signature V4.
    """
    REQUIRES_REGION = True

    def __init__(self, credentials, service_name, region_name):
        self.credentials = credentials
        # We initialize these value here so the unit tests can have
        # valid values.  But these will get overriden in ``add_auth``
        # later for real requests.
        self._region_name = region_name
        if service_name == 'sts':
            self._service_name = 's3'
        else:
            self._service_name = service_name

