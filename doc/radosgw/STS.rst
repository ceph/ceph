===========
STS in Ceph
===========

Secure Token Service is a web service in AWS that returns a set of temporary security credentials for authenticating federated users.
The link to official AWS documentation can be found here: https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html.

Ceph Object Gateway implements a subset of STS APIs that provide temporary credentials for identity and access management.
These temporary credentials can be used to make subsequent S3 calls which will be authenticated by the STS engine in Ceph Object Gateway.
Permissions of the temporary credentials can be further restricted via an IAM policy passed as a parameter to the STS APIs.

STS REST APIs
=============

The following STS REST APIs have been implemented in Ceph Object Gateway:

1. AssumeRole: Returns a set of temporary credentials that can be used for 
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

2. AssumeRoleWithWebIdentity: Returns a set of temporary credentials for users that
have been authenticated by a web/mobile app by an OpenID Connect /OAuth2.0 Identity Provider.
Currently Keycloak has been tested and integrated with RGW.

Parameters:
    **RoleArn** (String/ Required): ARN of the Role to Assume.

    **RoleSessionName** (String/ Required): An Identifier for the assumed role
    session.

    **Policy** (String/ Optional): An IAM Policy in JSON format.

    **DurationSeconds** (Integer/ Optional): The duration in seconds of the session.
    Its default value is 3600.

    **ProviderId** (String/ Optional): Fully qualified host component of the domain name
    of the IDP. Valid only for OAuth2.0 tokens (not for OpenID Connect tokens).

    **WebIdentityToken** (String/ Required): The OpenID Connect/ OAuth2.0 token, which the
    application gets in return after authenticating its user with an IDP.

Before invoking AssumeRoleWithWebIdentity, an OpenID Connect Provider entity (which the web application
authenticates with), needs to be created in RGW.

The trust between the IDP and the role is created by adding a Condition to the role trust policy, which
allows access only to applications with the app id given in the trust policy document. The Condition
is of the form::

    "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\"arn:aws:iam:::oidc-provider/<URL of IDP>\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"<URL of IDP> :app_id\":\"<aud>\"\}\}\}\]\}"

The app_id in the condition above must match the 'aud' field of the incoming token.

A shadow user is created corresponding to every federated user. The user id is derived from the 'sub' field of the incoming web token.
The user is created in a separate namespace - 'oidc' such that the user id doesn't clash with any other user ids in rgw. The format of the user id
is - <tenant>$<user-namespace>$<sub> where user-namespace is 'oidc' for users that authenticate with oidc providers.

STS Configuration
=================

The following configurable options have to be added for STS integration::

  [client.radosgw.gateway]
  rgw sts key = {sts key for encrypting the session token}
  rgw s3 auth use sts = true

Note: By default, STS and S3 APIs co-exist in the same namespace, and both S3
and STS APIs can be accessed via the same endpoint in Ceph Object Gateway.

Examples
========

1. The following is an example of AssumeRole API call, which shows steps to create a role, assign a policy to it
(that allows access to S3 resources), assuming a role to get temporary credentials and accessing s3 resources using
those credentials. In this example, TESTER1 assumes a role created by TESTER, to access S3 resources owned by TESTER,
according to the permission policy attached to the role.

.. code-block:: python

    import boto3

    iam_client = boto3.client('iam',
    aws_access_key_id=<access_key of TESTER>,
    aws_secret_access_key=<secret_key of TESTER>,
    endpoint_url=<IAM URL>,
    region_name=''
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/TESTER1\"]},\"Action\":[\"sts:AssumeRole\"]}]}"

    role_response = iam_client.create_role(
    AssumeRolePolicyDocument=policy_document,
    Path='/',
    RoleName='S3Access',
    )

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"

    response = iam_client.put_role_policy(
    RoleName='S3Access',
    PolicyName='Policy1',
    PolicyDocument=role_policy
    )

    sts_client = boto3.client('sts',
    aws_access_key_id=<access_key of TESTER1>,
    aws_secret_access_key=<secret_key of TESTER1>,
    endpoint_url=<STS URL>,
    region_name='',
    )

    response = sts_client.assume_role(
    RoleArn=role_response['Role']['Arn'],
    RoleSessionName='Bob',
    DurationSeconds=3600
    )

    s3client = boto3.client('s3',
    aws_access_key_id = response['Credentials']['AccessKeyId'],
    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
    aws_session_token = response['Credentials']['SessionToken'],
    endpoint_url=<S3 URL>,
    region_name='',)

    bucket_name = 'my-bucket'
    s3bucket = s3client.create_bucket(Bucket=bucket_name)
    resp = s3client.list_buckets()

2. The following is an example of AssumeRoleWithWebIdentity API call, where an external app that has users authenticated with
an OpenID Connect/ OAuth2 IDP (Keycloak in this example), assumes a role to get back temporary credentials and access S3 resources
according to permission policy of the role.

.. code-block:: python

    import boto3

    iam_client = boto3.client('iam',
    aws_access_key_id=<access_key of TESTER>,
    aws_secret_access_key=<secret_key of TESTER>,
    endpoint_url=<IAM URL>,
    region_name=''
    )

    oidc_response = iam_client.create_open_id_connect_provider(
        Url=<URL of the OpenID Connect Provider,
        ClientIDList=[
            <Client id registered with the IDP>
        ],
        ThumbprintList=[
            <Thumbprint of the IDP>
     ]
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\"arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/demo\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/demo:app_id\":\"customer-portal\"}}}]}"
    role_response = iam_client.create_role(
    AssumeRolePolicyDocument=policy_document,
    Path='/',
    RoleName='S3Access',
    )

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"

    response = iam_client.put_role_policy(
        RoleName='S3Access',
        PolicyName='Policy1',
        PolicyDocument=role_policy
    )

    sts_client = boto3.client('sts',
    aws_access_key_id=<access_key of TESTER1>,
    aws_secret_access_key=<secret_key of TESTER1>,
    endpoint_url=<STS URL>,
    region_name='',
    )

    response = client.assume_role_with_web_identity(
    RoleArn=role_response['Role']['Arn'],
    RoleSessionName='Bob',
    DurationSeconds=3600,
    WebIdentityToken=<Web Token>
    )

    s3client = boto3.client('s3',
    aws_access_key_id = response['Credentials']['AccessKeyId'],
    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
    aws_session_token = response['Credentials']['SessionToken'],
    endpoint_url=<S3 URL>,
    region_name='',)

    bucket_name = 'my-bucket'
    s3bucket = s3client.create_bucket(Bucket=bucket_name)
    resp = s3client.list_buckets()

How to obtain thumbprint of an OpenID Connect Provider IDP
==========================================================
1. Take the OpenID connect provider's URL and add /.well-known/openid-configuration
to it to get the URL to get the IDP's configuration document. For example, if the URL
of the IDP is http://localhost:8000/auth/realms/quickstart, then the URL to get the
document from is http://localhost:8000/auth/realms/quickstart/.well-known/openid-configuration

2. Use the following curl command to get the configuration document from the URL described
in step 1::

    curl -k -v \
      -X GET \
      -H "Content-Type: application/x-www-form-urlencoded" \
      "http://localhost:8000/auth/realms/quickstart/.well-known/openid-configuration" \
    | jq .

 3. From the response of step 2, use the value of "jwks_uri" to get the certificate of the IDP,
 using the following code::
     curl -k -v \
      -X GET \
      -H "Content-Type: application/x-www-form-urlencoded" \
      "http://$KC_SERVER/$KC_CONTEXT/realms/$KC_REALM/protocol/openid-connect/certs" \
      | jq .

3. Copy the result of "x5c" in the response above, in a file certificate.crt, and add
'-----BEGIN CERTIFICATE-----' at the beginning and "-----END CERTIFICATE-----"
at the end.

4. Use the following OpenSSL command to get the certificate thumbprint::

    openssl x509 -in certificate.crt -fingerprint -noout

5. The result of the above command in step 4, will be a SHA1 fingerprint, like the following::

    SHA1 Fingerprint=F7:D7:B3:51:5D:D0:D3:19:DD:21:9A:43:A9:EA:72:7A:D6:06:52:87

6.  Remove the colons from the result above to get the final thumbprint which can be as input
while creating the OpenID Connect Provider entity in IAM::

    F7D7B3515DD0D319DD219A43A9EA727AD6065287

Roles in RGW
============

More information for role manipulation can be found here
:doc:`role`.

OpenID Connect Provider in RGW
==============================

More information for OpenID Connect Provider entity manipulation
can be found here
:doc:`oidc`.

Keycloak integration with Radosgw
=================================

Steps for integrating Radosgw with Keycloak can be found here
:doc:`keycloak`.

STSLite
=======
STSLite has been built on STS, and documentation for the same can be found here
:doc:`STSLite`.