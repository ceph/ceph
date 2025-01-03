=======================================================
Session tags for Attribute Based Access Control in STS
=======================================================

Session tags are key-value pairs that can be passed while federating a user (currently it
is only supported as part of the web token passed to AssumeRoleWithWebIdentity). The session
tags are passed along as aws:PrincipalTag in the session credentials (temporary credentials)
that is returned back by STS. These Principal Tags consists of the session tags that come in
as part of the web token and the tags that are attached to the role being assumed. Please note
that the tags have to be always specified in the following namespace: https://aws.amazon.com/tags.

An example of the session tags that are passed in by the IDP in the web token is as follows:

.. code-block:: python

    {
        "jti": "947960a3-7e91-4027-99f6-da719b0d4059",
        "exp": 1627438044,
        "nbf": 0,
        "iat": 1627402044,
        "iss": "http://localhost:8080/auth/realms/quickstart",
        "aud": "app-profile-jsp",
        "sub": "test",
        "typ": "ID",
        "azp": "app-profile-jsp",
        "auth_time": 0,
        "session_state": "3a46e3e7-d198-4a64-8b51-69682bcfc670",
        "preferred_username": "test",
        "email_verified": false,
        "acr": "1",
        "https://aws.amazon.com/tags": [
            {
                "principal_tags": {
                    "Department": [
                        "Engineering",
                        "Marketing"
                    ]
                }
            }
        ],
        "client_id": "app-profile-jsp",
        "username": "test",
        "active": true
    }

Steps to configure Keycloak to pass tags in the web token are described here:
:ref:`radosgw_keycloak`.

The trust policy must have 'sts:TagSession' permission if the web token passed
in by the federated user contains session tags, otherwise the
AssumeRoleWithWebIdentity action will fail. An example of the trust policy with
sts:TagSession is as follows:

.. code-block:: python

    {
	    "Version":"2012-10-17",
	    "Statement":[
	    {
	        "Effect":"Allow",
	        "Action":["sts:AssumeRoleWithWebIdentity","sts:TagSession"],
	        "Principal":{"Federated":["arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"]},
	        "Condition":{"StringEquals":{"localhost:8080/auth/realms/quickstart:sub":"test"}}
	    }]
	}

Tag Keys
========

The following are the tag keys that can be used in the role's trust policy or the role's permission policy:

1. aws:RequestTag: This key is used to compare the key-value pair passed in the request with the key-value pair
in the role's trust policy. In case of AssumeRoleWithWebIdentity, the session tags that are passed by the idp
in the web token can be used as aws:RequestTag in the role's trust policy based on which a federated user can be
allowed to assume a role.

An example of a role trust policy that uses aws:RequestTag is as follows:

.. code-block:: python

    {
	    "Version":"2012-10-17",
	    "Statement":[
	    {
	        "Effect":"Allow",
	        "Action":["sts:AssumeRoleWithWebIdentity","sts:TagSession"],
	        "Principal":{"Federated":["arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"]},
	        "Condition":{"StringEquals":{"aws:RequestTag/Department":"Engineering"}}
	    }]
	}

2. aws:PrincipalTag: This key is used to compare the key-value pair attached to the principal with the key-value pair
in the policy. In case of AssumeRoleWithWebIdentity, the session tags that are passed by the idp in the web token appear
as Principal tags in the temporary credentials once a user has been authenticated, and these tags can be used as
aws:PrincipalTag in the role's permission policy.

An example of a role permission policy that uses aws:PrincipalTag is as follows:

.. code-block:: python

    {
	    "Version":"2012-10-17",
	    "Statement":[
	    {
	        "Effect":"Allow",
	        "Action":["s3:*"],
            "Resource":["arn:aws:s3::t1tenant:my-test-bucket","arn:aws:s3::t1tenant:my-test-bucket/*"],
	        "Condition":{"StringEquals":{"aws:PrincipalTag/Department":"Engineering"}}
	    }]
	}

3. iam:ResourceTag: This key is used to compare the key-value pair attached to the resource with the key-value pair
in the policy. In case of AssumeRoleWithWebIdentity, tags attached to the role can be used to compare with that in
the trust policy to allow a user to assume a role.
RGW now supports REST APIs for tagging, listing tags and untagging actions on a role. More information related to
role tagging can be found here :doc:`role`.

An example of a role's trust policy that uses aws:ResourceTag is as follows:

.. code-block:: python

    {
	    "Version":"2012-10-17",
	    "Statement":[
	    {
	        "Effect":"Allow",
	        "Action":["sts:AssumeRoleWithWebIdentity","sts:TagSession"],
	        "Principal":{"Federated":["arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"]},
	        "Condition":{"StringEquals":{"iam:ResourceTag/Department":"Engineering"}}
	    }]
	}

For the above to work, you need to attach 'Department=Engineering' tag to the role.

4. aws:TagKeys: This key is used to compare tags in the request with the tags in the policy. In case of
AssumeRoleWithWebIdentity this can be used to check the tag keys in a role's trust policy before a user
is allowed to assume a role.
This can also be used in the role's permission policy.

An example of a role's trust policy that uses aws:TagKeys is as follows:

.. code-block:: python

    {
	    "Version":"2012-10-17",
	    "Statement":[
	    {
	        "Effect":"Allow",
	        "Action":["sts:AssumeRoleWithWebIdentity","sts:TagSession"],
	        "Principal":{"Federated":["arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"]},
	        "Condition":{"ForAllValues:StringEquals":{"aws:TagKeys":["Department"]}}
	    }]
	}

'ForAllValues:StringEquals' tests whether every tag key in the request is a subset of the tag keys in the policy. So the above
condition restricts the tag keys passed in the request.

5. s3:ResourceTag: This key is used to compare tags present on the s3 resource (bucket or object) with the tags in
the role's permission policy.

An example of a role's permission policy that uses s3:ResourceTag is as follows:

.. code-block:: python

    {
        "Version":"2012-10-17",
        "Statement":[
        {
            "Effect":"Allow",
            "Action":["s3:PutBucketTagging"],
            "Resource":["arn:aws:s3::t1tenant:my-test-bucket\","arn:aws:s3::t1tenant:my-test-bucket/*"]
        },
        {
            "Effect":"Allow",
            "Action":["s3:*"],
            "Resource":["*"],
            "Condition":{"StringEquals":{"s3:ResourceTag/Department":\"Engineering"}}
        }
    }

For the above to work, you need to attach 'Department=Engineering' tag to the bucket (and on the object too) on which you want this policy
to be applied.

More examples of policies using tags
====================================

1. To assume a role by matching the tags in the incoming request with the tag attached to the role.
aws:RequestTag is the incoming tag in the JWT (access token) and iam:ResourceTag is the tag attached to the role being assumed:

.. code-block:: python

    {
	    "Version":"2012-10-17",
	    "Statement":[
	    {
	        "Effect":"Allow",
	        "Action":["sts:AssumeRoleWithWebIdentity","sts:TagSession"],
	        "Principal":{"Federated":["arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"]},
	        "Condition":{"StringEquals":{"aws:RequestTag/Department":"${iam:ResourceTag/Department}"}}
	    }]
	}

2. To evaluate a role's permission policy by matching principal tags with s3 resource tags.
aws:PrincipalTag is the tag passed in along with the temporary credentials and s3:ResourceTag is the tag attached to
the s3 resource (object/ bucket):

.. code-block:: python


    {
        "Version":"2012-10-17",
        "Statement":[
        {
            "Effect":"Allow",
            "Action":["s3:PutBucketTagging"],
            "Resource":["arn:aws:s3::t1tenant:my-test-bucket\","arn:aws:s3::t1tenant:my-test-bucket/*"]
        },
        {
            "Effect":"Allow",
            "Action":["s3:*"],
            "Resource":["*"],
            "Condition":{"StringEquals":{"s3:ResourceTag/Department":"${aws:PrincipalTag/Department}"}}
        }
    }

Properties of Session Tags
==========================

1. Session Tags can be multi-valued. (Multi-valued session tags are not supported in AWS)
2. A maximum of 50 session tags are allowed to be passed in by the IDP.
3. The maximum size of a key allowed is 128 characters.
4. The maximum size of a value allowed is 256 characters.
5. The tag or the value can not start with "aws:".

s3 Resource Tags
================

As stated above 's3:ResourceTag' key can be used for authorizing an s3 operation in RGW (this is not allowed in AWS).

s3:ResourceTag is a key used to refer to tags that have been attached to an object or a bucket. Tags can be attached to an object or
a bucket using REST APIs available for the same.

The following table shows which s3 resource tag type (bucket/object) are supported for authorizing a particular operation.

+-----------------------------------+-------------------+
| Operation                         | Tag type          |
+===================================+===================+
| **GetObject**                     | Object tags       |
| **GetObjectTags**                 |                   |
| **DeleteObjectTags**              |                   |
| **DeleteObject**                  |                   |
| **PutACLs**                       |                   |
| **InitMultipart**                 |                   |
| **AbortMultipart**                |                   |
| **ListMultipart**                 |                   |
| **GetAttrs**                      |                   |
| **PutObjectRetention**            |                   |
| **GetObjectRetention**            |                   |
| **PutObjectLegalHold**            |                   |
| **GetObjectLegalHold**            |                   |
+-----------------------------------+-------------------+
| **PutObjectTags**                 | Bucket tags       |
| **GetBucketTags**                 |                   |
| **PutBucketTags**                 |                   |
| **DeleteBucketTags**              |                   |
| **GetBucketReplication**          |                   |
| **DeleteBucketReplication**       |                   |
| **GetBucketVersioning**           |                   |
| **SetBucketVersioning**           |                   |
| **GetBucketWebsite**              |                   |
| **SetBucketWebsite**              |                   |
| **DeleteBucketWebsite**           |                   |
| **StatBucket**                    |                   |
| **ListBucket**                    |                   |
| **GetBucketLogging**              |                   |
| **GetBucketLocation**             |                   |
| **DeleteBucket**                  |                   |
| **GetLC**                         |                   |
| **PutLC**                         |                   |
| **DeleteLC**                      |                   |
| **GetCORS**                       |                   |
| **PutCORS**                       |                   |
| **GetRequestPayment**             |                   |
| **SetRequestPayment**             |                   |
| **PutBucketPolicy**               |                   |
| **GetBucketPolicy**               |                   |
| **DeleteBucketPolicy**            |                   |
| **PutBucketObjectLock**           |                   |
| **GetBucketObjectLock**           |                   |
| **GetBucketPolicyStatus**         |                   |
| **PutBucketPublicAccessBlock**    |                   |
| **GetBucketPublicAccessBlock**    |                   |
| **DeleteBucketPublicAccessBlock** |                   |
+-----------------------------------+-------------------+
| **GetACLs**                       | Bucket tags for   |
| **PutACLs**                       | bucket ACLs       |
|                                   | Object tags for   |
|                                   | object ACLs       |
+-----------------------------------+-------------------+
| **PutObject**                     | Object tags of    |
| **CopyObject**                    | source object     |
|                                   | Bucket tags of    |
|                                   | destination bucket|
+-----------------------------------+-------------------+


Sample code demonstrating usage of session tags
===============================================

The following is a sample code for tagging a role, a bucket, an object in it and using tag keys in a role's
trust policy and its permission policy, assuming that a tag 'Department=Engineering' is passed in the
JWT (access token) by the IDP

.. code-block:: python

    # -*- coding: utf-8 -*-

    import boto3
    import json
    from nose.tools import eq_ as eq

    access_key = 'TESTER'
    secret_key = 'test123'
    endpoint = 'http://s3.us-east.localhost:8000'

    s3client = boto3.client('s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    endpoint_url = endpoint,
    region_name='',)

    s3res = boto3.resource('s3',
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            endpoint_url = endpoint,
            region_name='',)

    iam_client = boto3.client('iam',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint,
    region_name=''
    )

    bucket_name = 'test-bucket'
    s3bucket = s3client.create_bucket(Bucket=bucket_name)

    bucket_tagging = s3res.BucketTagging(bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'Engineering'}]})
    try:
        response = iam_client.create_open_id_connect_provider(
            Url='http://localhost:8080/auth/realms/quickstart',
            ClientIDList=[
                'app-profile-jsp',
                'app-jee-jsp'
            ],
            ThumbprintList=[
                'F7D7B3515DD0D319DD219A43A9EA727AD6065287'
        ]
        )
    except ClientError as e:
        print ("Provider already exists")

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\"arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"${iam:ResourceTag/Department}\"}}}]}"
    role_response = ""

    print ("\n Getting Role \n")

    try:
        role_response = iam_client.get_role(
            RoleName='S3Access'
        )
        print (role_response)
    except ClientError as e:
        if e.response['Code'] == 'NoSuchEntity':
            print ("\n Creating Role \n")
            tags_list = [
                {'Key':'Department','Value':'Engineering'},
            ]
            role_response = iam_client.create_role(
                AssumeRolePolicyDocument=policy_document,
                Path='/',
                RoleName='S3Access',
                Tags=tags_list,
            )
            print (role_response)
        else:
            print("Unexpected error: %s" % e)

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"${aws:PrincipalTag/Department}\"]}}}}"

    response = iam_client.put_role_policy(
                RoleName='S3Access',
                PolicyName='Policy1',
                PolicyDocument=role_policy
            )

    sts_client = boto3.client('sts',
    aws_access_key_id='abc',
    aws_secret_access_key='def',
    endpoint_url = endpoint,
    region_name = '',
    )


    print ("\n Assuming Role with Web Identity\n")
    response = sts_client.assume_role_with_web_identity(
    RoleArn=role_response['Role']['Arn'],
    RoleSessionName='Bob',
    DurationSeconds=900,
    WebIdentityToken='<web-token>')

    s3client2 = boto3.client('s3',
    aws_access_key_id = response['Credentials']['AccessKeyId'],
    aws_secret_access_key = response['Credentials']['SecretAccessKey'],
    aws_session_token = response['Credentials']['SessionToken'],
    endpoint_url='http://s3.us-east.localhost:8000',
    region_name='',)

    bucket_body = 'this is a test file'
    tags = 'Department=Engineering'
    key = "test-1.txt"
    s3_put_obj = s3client2.put_object(Body=bucket_body, Bucket=bucket_name, Key=key, Tagging=tags)
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    s3_get_obj = s3client2.get_object(Bucket=bucket_name, Key=key)
    eq(s3_get_obj['ResponseMetadata']['HTTPStatusCode'],200)
