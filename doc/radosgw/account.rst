===============
 User Accounts
===============

.. versionadded:: Squid

The Ceph Object Gateway supports *user accounts* as an optional feature to
enable the self-service management of :ref:`Users <radosgw-user-management>`,
Groups and `Roles`_ similar to those in `AWS Identity and Access Management`_
(IAM).

.. _radosgw-account-root-user:

Account Root User
=================

Each account is managed by an *account root user*. Like normal users and roles,
accounts and account root users must be created by an administrator using
``radosgw-admin`` or the `Admin Ops API`_.

The account root user has default permissions on all resources owned by
the account. The root user's credentials (access and secret keys) can be
used with the `Ceph Object Gateway IAM API`_ to create additional IAM users
and roles for use with the `Ceph Object Gateway S3 API`_, as well as to
manage their associated access keys and policies.

Account owners are encouraged to use this account root user for management
only, and create users and roles with fine-grained permissions for specific
applications.

.. warning:: While the account root user does not require IAM policy to
   access resources within the account, it is possible to add policy that
   denies their access explicitly. Use Deny statements with caution.

Resource Ownership
==================

When a normal (non-account) user creates buckets and uploads objects, those
resources are owned by the user. The associated S3 ACLs name that user as
both the owner and grantee, and those buckets are only visible to the owning
user in a ``s3:ListBuckets`` request.

In contrast, when users or roles belong to an account, the resources they
create are instead owned by the account itself. The associated S3 ACLs name
the account id as the owner and grantee, and those buckets are visible to
``s3:ListBuckets`` requests sent by any user or role in that account.

Because the resources are owned by the account rather than its users, all
usage statistics and quota enforcement apply to the account as a whole rather
than its individual users.

Account IDs
===========

Account identifiers can be used in several places that otherwise accept
User IDs or tenant names, so Account IDs use a special format to avoid
ambiguity: the string ``RGW`` followed by 17 numeric digits like
``RGW33567154695143645``. An Account ID in that format is randomly generated
upon account creation if one is not specified.

Account IDs are commonly found in the `Amazon Resource Names`_ (ARNs) of IAM
policy documents. For example, ``arn:aws:iam::RGW33567154695143645:user/A``
refers to an IAM user named A in that account. The Ceph Object Gateway also
supports tenant names in that position.

Accounts IDs can also be used in ACLs for a ``Grantee`` of type ``CanonicalUser``.
User IDs are also supported here.

IAM Policy
==========

While non-account users are allowed to create buckets and upload objects by
default, account users start with no permissions at all.

Before an IAM user can perform API operations, some policy must be added to
allow it. The account root user can add identity policies to its users in
several ways.

* Add policy directly to the user with the ``iam:PutUserPolicy`` and
  ``iam:AttachUserPoliicy`` actions.

* Create an IAM group and add group policy with the ``iam:PutGroupPolicy`` and
  ``iam:AttachGroupPoliicy`` actions. Users added to that group with the
  ``iam:AddUserToGroup`` action will inherit all of the group's policy.

* Create an IAM role and add role policy with the ``iam:PutRolePolicy`` and
  ``iam:AttachRolePoliicy`` actions. Users that assume this role with the
  ``sts:AssumeRole`` and ``sts:AssumeRoleWithWebIdentity`` actions will inherit
  all of the role's policy.

These identity policies are evaluated according to the rules in
`Evaluating policies within a single account`_ and
`Cross-account policy evaluation logic`_.

Principals
----------

The "Principal" ARNs in policy documents refer to users differently when they
belong to an account.

Outside of an account, user principals are named by user id such as
``arn:aws:iam:::user/uid`` or ``arn:aws:iam::tenantname:user/uid``, where
``uid`` corresponds to the ``--uid`` argument from ``radosgw-admin``.

Within an account, user principals instead use the user name, such as
``arn:aws:iam::RGW33567154695143645:user/name`` where ``name`` corresponds
to the ``--display-name`` argument from ``radosgw-admin``. Account users
continue to match the tenant form so that existing policy continues to work
when users are migrated into accounts.

Tenant Isolation
================

Like users, accounts can optionally belong to a tenant for namespace isolation
of buckets. For example, one account named "acct" can exist under a tenant "a",
and a different account named "acct" can exist under tenant "b". Refer to
:ref:`Multitenancy <rgw-multitenancy>` for details.

A tenanted account can only contain users with the same tenant name.

Regardless of tenant, account IDs and email addresses must be globally unique.

Account Management
==================

Create an Account
-----------------

To create an account::

	radosgw-admin account create [--account-name={name}] [--account-id={id}] [--email={email}]

Create an Account Root User
---------------------------

To create an account root user::

	radosgw-admin user create --uid={userid} --display-name={name} --account-id={accountid} --account-root --gen-secret --gen-access-key

Delete an Account
-----------------

To delete an account::

	radosgw-admin account rm --account-id={accountid}

Account Stats/Quota
-------------------

To view account stats::

	radosgw-admin account stats --account-id={accountid} --sync-stats

To enable an account quota::

	radosgw-admin quota set --quota-scope=account --account-id={accountid} --max-size=10G
	radosgw-admin quota enable --quota-scope=account --account-id={accountid}

To enable a bucket quota for the account::

	radosgw-admin quota set --quota-scope=bucket --account-id={accountid} --max-objects=1000000
	radosgw-admin quota enable --quota-scope=bucket --account-id={accountid}

Migrate an existing User into an Account
----------------------------------------

An existing user can be adopted into an account with ``user modify``::

	radosgw-admin user modify --uid={userid} --account-id={accountid}

.. note:: Ownership of all of the user's buckets will be transferred to
   the account.

.. note:: Account membership is permanent. Once added, users cannot be
   removed from their account.

.. warning:: Ownership of the user's notification topics will not be
   transferred to the account. Notifications will continue to work, but
   the topics will no longer be visible to SNS Topic APIs. Topics and
   their associated bucket notifications should be removed before migration
   and recreated within the account.

Because account users have no permissions by default, some identity policy must
be added to restore the user's original permissions.

Alternatively, you may want to create a new account for each existing user. In
that case, you may want to add the ``--account-root`` option to make each user
the root user of their account.

Account Root example
--------------------

The account root user's credentials unlock the `Ceph Object Gateway IAM API`_.

This example uses `awscli`_ to create an IAM user for S3 operations.

1. Create a profile for the account root user::

	$ aws --profile rgwroot configure set endpoint_url http://localhost:8000
	$ aws --profile rgwroot configure
	AWS Access Key ID [None]: {root access key}
	AWS Secret Access Key [None]: {root secret key}
	Default region name [None]: default
	Default output format [None]:

2. Create an IAM user, add credentials, and attach a policy for S3 access::

	$ aws --profile rgwroot iam create-user --user-name Alice
	{
	    "User": {
	        "Path": "/",
	        "UserName": "Alice",
	        "UserId": "b580aa8e-14c7-4b6a-9dac-a30c640244b6",
	        "Arn": "arn:aws:iam::RGW63136524507535818:user/Alice",
	        "CreateDate": "2024-02-07T00:15:45.162786+00:00"
	    }
	}
	$ aws --profile rgwroot iam create-access-key --user-name Alice
	{
	    "AccessKey": {
	        "UserName": "Alice",
	        "AccessKeyId": "JBNLYD5BDNRVV64J02E8",
	        "Status": "Active",
	        "SecretAccessKey": "SnHoE700kdNuT22K8Bhy2iL3DwZU0sUSDI1gUXHr",
	        "CreateDate": "2024-02-07T00:16:34.679316+00:00"
	    }
	}
	$ aws --profile rgwroot iam attach-user-policy --user-name Alice \
	      --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

3. Create a profile for the S3 user::

	$ aws --profile rgws3 configure set endpoint_url http://localhost:8000
	$ aws --profile rgws3 configure
	AWS Access Key ID [None]: JBNLYD5BDNRVV64J02E8
	AWS Secret Access Key [None]: SnHoE700kdNuT22K8Bhy2iL3DwZU0sUSDI1gUXHr
	Default region name [None]: default
	Default output format [None]:

4. Use the S3 user profile to create a bucket::

	$ aws --profile rgws3 s3 mb s3://testbucket
	make_bucket: testbucket


.. _Roles: ../role/
.. _AWS Identity and Access Management: https://aws.amazon.com/iam/
.. _Ceph Object Gateway IAM API: ../iam/
.. _Admin Ops API: ../adminops/
.. _Ceph Object Gateway S3 API: ../s3/
.. _Amazon Resource Names: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
.. _Evaluating policies within a single account: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html#policy-eval-basics
.. _Cross-account policy evaluation logic: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic-cross-account.html
.. _awscli: https://docs.aws.amazon.com/cli/latest/
