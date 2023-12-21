======
 Role
======

A role is similar to a user. It has permission policies attached to it that
determine what it can do and what it cannot do. A role can be assumed by any
identity that needs it. When a user assumes a role, a set of
dynamically-created temporary credentials are provided to the user. A role can
be used to delegate access to users, to applications, and to services that do
not have permissions to access certain S3 resources.

The following ``radosgw-admin`` commands can be used to create or delete or
update a role and the permissions associated with it.

Create a Role
-------------

To create a role, run a command of the following form::

	radosgw-admin role create --role-name={role-name} [--path=="{path to the role}"] [--assume-role-policy-doc={trust-policy-document}]

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

``path``

:Description: Path to the role. The default value is a slash(``/``).
:Type: String

``assume-role-policy-doc``

:Description: The trust relationship policy document that grants an entity
              permission to assume the role.
:Type: String

For example::
	
  radosgw-admin role create --role-name=S3Access1 --path=/application_abc/component_xyz/ --assume-role-policy-doc=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Allow\",\"Principal\":\{\"AWS\":\[\"arn:aws:iam:::user/TESTER\"\]\},\"Action\":\[\"sts:AssumeRole\"\]\}\]\}
  
.. code-block:: javascript
  
  {
    "id": "ca43045c-082c-491a-8af1-2eebca13deec",
    "name": "S3Access1",
    "path": "/application_abc/component_xyz/",
    "arn": "arn:aws:iam:::role/application_abc/component_xyz/S3Access1",
    "create_date": "2018-10-17T10:18:29.116Z",
    "max_session_duration": 3600,
    "assume_role_policy_document": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/TESTER\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
  }


Delete a Role
-------------

To delete a role, run a command of the following form:

.. prompt:: bash

   radosgw-admin role delete --role-name={role-name}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

For example:

.. prompt:: bash
	
   radosgw-admin role delete --role-name=S3Access1

Note: A role can be deleted only when it has no permission policy attached to
it.

Get a Role
----------

To get information about a role, run a command of the following form:

.. prompt:: bash

   radosgw-admin role get --role-name={role-name}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

For example:

.. prompt:: bash
	
   radosgw-admin role get --role-name=S3Access1
  
.. code-block:: javascript
  
  {
    "id": "ca43045c-082c-491a-8af1-2eebca13deec",
    "name": "S3Access1",
    "path": "/application_abc/component_xyz/",
    "arn": "arn:aws:iam:::role/application_abc/component_xyz/S3Access1",
    "create_date": "2018-10-17T10:18:29.116Z",
    "max_session_duration": 3600,
    "assume_role_policy_document": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/TESTER\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
  }


List Roles
----------

To list roles with a specified path prefix, run a command of the following form:

.. prompt:: bash

   radosgw-admin role list [--path-prefix ={path prefix}]

Request Parameters
~~~~~~~~~~~~~~~~~~

``path-prefix``

:Description: Path prefix for filtering roles. If this is not specified, all
              roles are listed.
:Type: String

For example:

.. prompt:: bash
	
   radosgw-admin role list --path-prefix="/application"
  
.. code-block:: javascript
  
  [
    {
        "id": "3e1c0ff7-8f2b-456c-8fdf-20f428ba6a7f",
        "name": "S3Access1",
        "path": "/application_abc/component_xyz/",
        "arn": "arn:aws:iam:::role/application_abc/component_xyz/S3Access1",
        "create_date": "2018-10-17T10:32:01.881Z",
        "max_session_duration": 3600,
        "assume_role_policy_document": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/TESTER\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
    }
  ]

Update Assume Role Policy Document of a role
--------------------------------------------

To modify a role's assume role policy document, execute the following::

	radosgw-admin role-trust-policy modify --role-name={role-name} --assume-role-policy-doc={trust-policy-document}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

``assume-role-policy-doc``

:Description: The trust relationship policy document that grants an entity permission to assume the role.
:Type: String

For example::

  radosgw-admin role-trust-policy modify --role-name=S3Access1 --assume-role-policy-doc=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Allow\",\"Principal\":\{\"AWS\":\[\"arn:aws:iam:::user/TESTER2\"\]\},\"Action\":\[\"sts:AssumeRole\"\]\}\]\}

.. code-block:: javascript

  {
    "id": "ca43045c-082c-491a-8af1-2eebca13deec",
    "name": "S3Access1",
    "path": "/application_abc/component_xyz/",
    "arn": "arn:aws:iam:::role/application_abc/component_xyz/S3Access1",
    "create_date": "2018-10-17T10:18:29.116Z",
    "max_session_duration": 3600,
    "assume_role_policy_document": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/TESTER2\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
  }


In the above example, we are modifying the Principal from TESTER to TESTER2 in its assume role policy document.

Add/ Update a Policy attached to a Role
---------------------------------------

To add or update the inline policy attached to a role, execute the following::

	radosgw-admin role policy put --role-name={role-name} --policy-name={policy-name} --policy-doc={permission-policy-doc}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

``policy-name``

:Description: Name of the policy.
:Type: String

``policy-doc``

:Description: The Permission policy document.
:Type: String

For example::

  radosgw-admin role-policy put --role-name=S3Access1 --policy-name=Policy1 --policy-doc=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Allow\",\"Action\":\[\"s3:*\"\],\"Resource\":\"arn:aws:s3:::example_bucket\"\}\]\}

For passing ``policy-doc`` as a file::

  radosgw-admin role-policy put --role-name=S3Access1 --policy-name=Policy1 --infile policy-document.json

In the above example, we are attaching a policy 'Policy1' to role 'S3Access1', which allows all s3 actions on 'example_bucket'.

List Permission Policy Names attached to a Role
-----------------------------------------------

To list the names of permission policies attached to a role, execute the following::

	radosgw-admin role policy get --role-name={role-name}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

For example::

  radosgw-admin role-policy list --role-name=S3Access1

.. code-block:: javascript

  [
    "Policy1"
  ]


Get Permission Policy attached to a Role
----------------------------------------

To get a specific permission policy attached to a role, execute the following::

	radosgw-admin role policy get --role-name={role-name} --policy-name={policy-name}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

``policy-name``

:Description: Name of the policy.
:Type: String

For example::

  radosgw-admin role-policy get --role-name=S3Access1 --policy-name=Policy1

.. code-block:: javascript

  {
    "Permission policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:*\"],\"Resource\":\"arn:aws:s3:::example_bucket\"}]}"
  }


Delete Policy attached to a Role
--------------------------------

To delete permission policy attached to a role, execute the following::

	radosgw-admin role policy delete --role-name={role-name} --policy-name={policy-name}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

``policy-name``

:Description: Name of the policy.
:Type: String

For example::

  radosgw-admin role-policy delete --role-name=S3Access1 --policy-name=Policy1


Update a role
-------------

To update a role's max-session-duration, execute the following::

	radosgw-admin role update --role-name={role-name} --max-session-duration={max-session-duration}

Request Parameters
~~~~~~~~~~~~~~~~~~

``role-name``

:Description: Name of the role.
:Type: String

``max-session-duration``

:Description: Maximum session duration for a role.
:Type: String

For example::

  radosgw-admin role update --role-name=S3Access1 --max-session-duration=43200

Note: This command currently can only be used to update max-session-duration.

REST APIs for Manipulating a Role
=================================

In addition to the above radosgw-admin commands, the following REST APIs can be used for manipulating a role. For the request parameters and their explanations, refer to the sections above.

In order to invoke the REST admin APIs, a user with admin caps needs to be created.

.. code-block:: javascript

  radosgw-admin --uid TESTER --display-name "TestUser" --access_key TESTER --secret test123 user create
  radosgw-admin caps add --uid="TESTER" --caps="roles=*"


Create a Role
-------------

Example::

  POST "<hostname>?Action=CreateRole&RoleName=S3Access&Path=/application_abc/component_xyz/&AssumeRolePolicyDocument=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Allow\",\"Principal\":\{\"AWS\":\[\"arn:aws:iam:::user/TESTER\"\]\},\"Action\":\[\"sts:AssumeRole\"\]\}\]\}"

.. code-block:: XML

  <role>
    <id>8f41f4e0-7094-4dc0-ac20-074a881ccbc5</id>
    <name>S3Access</name>
    <path>/application_abc/component_xyz/</path>
    <arn>arn:aws:iam:::role/application_abc/component_xyz/S3Access</arn>
    <create_date>2018-10-23T07:43:42.811Z</create_date>
    <max_session_duration>3600</max_session_duration>
    <assume_role_policy_document>{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/TESTER"]},"Action":["sts:AssumeRole"]}]}</assume_role_policy_document>
  </role>


Delete a Role
-------------

Example::

  POST "<hostname>?Action=DeleteRole&RoleName=S3Access"

Note: A role can be deleted only when it doesn't have any permission policy
attached to it. If you intend to delete a role, you must first delete any
policies attached to it.

Get a Role
----------

Example::

  POST "<hostname>?Action=GetRole&RoleName=S3Access"

.. code-block:: XML

  <role>
    <id>8f41f4e0-7094-4dc0-ac20-074a881ccbc5</id>
    <name>S3Access</name>
    <path>/application_abc/component_xyz/</path>
    <arn>arn:aws:iam:::role/application_abc/component_xyz/S3Access</arn>
    <create_date>2018-10-23T07:43:42.811Z</create_date>
    <max_session_duration>3600</max_session_duration>
    <assume_role_policy_document>{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/TESTER"]},"Action":["sts:AssumeRole"]}]}</assume_role_policy_document>
  </role>


List Roles
----------

Example::

  POST "<hostname>?Action=ListRoles&RoleName=S3Access&PathPrefix=/application"

.. code-block:: XML

  <role>
    <id>8f41f4e0-7094-4dc0-ac20-074a881ccbc5</id>
    <name>S3Access</name>
    <path>/application_abc/component_xyz/</path>
    <arn>arn:aws:iam:::role/application_abc/component_xyz/S3Access</arn>
    <create_date>2018-10-23T07:43:42.811Z</create_date>
    <max_session_duration>3600</max_session_duration>
    <assume_role_policy_document>{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/TESTER"]},"Action":["sts:AssumeRole"]}]}</assume_role_policy_document>
  </role>


Update Assume Role Policy Document
----------------------------------

Example::

  POST "<hostname>?Action=UpdateAssumeRolePolicy&RoleName=S3Access&PolicyDocument=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Allow\",\"Principal\":\{\"AWS\":\[\"arn:aws:iam:::user/TESTER2\"\]\},\"Action\":\[\"sts:AssumeRole\"\]\}\]\}"

Add/ Update a Policy attached to a Role
---------------------------------------

Example::

  POST "<hostname>?Action=PutRolePolicy&RoleName=S3Access&PolicyName=Policy1&PolicyDocument=\{\"Version\":\"2012-10-17\",\"Statement\":\[\{\"Effect\":\"Allow\",\"Action\":\[\"s3:CreateBucket\"\],\"Resource\":\"arn:aws:s3:::example_bucket\"\}\]\}"

List Permission Policy Names attached to a Role
-----------------------------------------------

Example::

  POST "<hostname>?Action=ListRolePolicies&RoleName=S3Access"

.. code-block:: XML

  <PolicyNames>
    <member>Policy1</member>
  </PolicyNames>


Get Permission Policy attached to a Role
----------------------------------------

Example::

  POST "<hostname>?Action=GetRolePolicy&RoleName=S3Access&PolicyName=Policy1"

.. code-block:: XML

  <GetRolePolicyResult>
    <PolicyName>Policy1</PolicyName>
    <RoleName>S3Access</RoleName>
    <Permission_policy>{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:CreateBucket"],"Resource":"arn:aws:s3:::example_bucket"}]}</Permission_policy>
  </GetRolePolicyResult>


Delete Policy attached to a Role
--------------------------------

Example::

  POST "<hostname>?Action=DeleteRolePolicy&RoleName=S3Access&PolicyName=Policy1"

Tag a role
----------
A role can have multivalued tags attached to it. These tags can be passed in as part of CreateRole REST API also.
AWS does not support multi-valued role tags.

Example::

  POST "<hostname>?Action=TagRole&RoleName=S3Access&Tags.member.1.Key=Department&Tags.member.1.Value=Engineering"

.. code-block:: XML

  <TagRoleResponse>
    <ResponseMetadata>
      <RequestId>tx000000000000000000004-00611f337e-1027-default</RequestId>
    </ResponseMetadata>
  </TagRoleResponse>


List role tags
--------------
Lists the tags attached to a role.

Example::

  POST "<hostname>?Action=ListRoleTags&RoleName=S3Access"

.. code-block:: XML

  <ListRoleTagsResponse>
    <ListRoleTagsResult>
      <Tags>
        <member>
          <Key>Department</Key>
          <Value>Engineering</Value>
        </member>
      </Tags>
    </ListRoleTagsResult>
    <ResponseMetadata>
      <RequestId>tx000000000000000000005-00611f337e-1027-default</RequestId>
    </ResponseMetadata>
  </ListRoleTagsResponse>

Delete role tags
----------------
Delete a tag/ tags attached to a role.

Example::

  POST "<hostname>?Action=UntagRoles&RoleName=S3Access&TagKeys.member.1=Department"

.. code-block:: XML

  <UntagRoleResponse>
    <ResponseMetadata>
      <RequestId>tx000000000000000000007-00611f337e-1027-default</RequestId>
    </ResponseMetadata>
  </UntagRoleResponse>

Update Role
-----------

Example::

  POST "<hostname>?Action=UpdateRole&RoleName=S3Access&MaxSessionDuration=43200"

.. code-block:: XML

  <UpdateRoleResponse>
    <UpdateRoleResult>
      <ResponseMetadata>
        <RequestId>tx000000000000000000007-00611f337e-1027-default</RequestId>
      </ResponseMetadata>
      </UpdateRoleResult>
  </UpdateRoleResponse>

Note: This API currently can only be used to update max-session-duration.

Sample code for tagging, listing tags and untagging a role
----------------------------------------------------------

The following is sample code for adding tags to role, listing tags and untagging a role using boto3.

.. code-block:: python

    import boto3

    access_key = 'TESTER'
    secret_key = 'test123'

    iam_client = boto3.client('iam',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url='http://s3.us-east.localhost:8000',
    region_name=''
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\"arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/quickstart:sub\":\"user1\"}}}]}"

    print ("\n Creating Role with tags\n")
    tags_list = [
        {'Key':'Department','Value':'Engineering'}
    ]
    role_response = iam_client.create_role(
        AssumeRolePolicyDocument=policy_document,
        Path='/',
        RoleName='S3Access',
        Tags=tags_list,
    )

    print ("Adding tags to role\n")
    response = iam_client.tag_role(
                RoleName='S3Access',
                Tags= [
                        {'Key':'CostCenter','Value':'123456'}
                    ]
                )
    print ("Listing role tags\n")
    response = iam_client.list_role_tags(
                RoleName='S3Access'
                )
    print (response)
    print ("Untagging role\n")
    response = iam_client.untag_role(
        RoleName='S3Access',
        TagKeys=[
            'Department',
        ]
    )
