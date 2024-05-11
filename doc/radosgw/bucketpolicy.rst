===============
Bucket Policies
===============

*Bucket policies were added in the Luminous release of Ceph.*

The Ceph Object Gateway supports a subset of the Amazon S3 policy
language applied to buckets.


Creation and Removal
====================

Bucket policies are managed through standard S3 operations rather than
radosgw-admin.

For example, one may use s3cmd to set or delete a policy thus::

  $ cat > examplepol
  {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"AWS": ["arn:aws:iam::usfolks:user/fred:subuser"]},
      "Action": "s3:PutObjectAcl",
      "Resource": [
        "arn:aws:s3:::happybucket/*"
      ]
    }]
  }

  $ s3cmd setpolicy examplepol s3://happybucket
  $ s3cmd delpolicy s3://happybucket


Limitations
===========

.. note:: This list of S3 actions is accurate only for the Reef release of
   Ceph. If you are using a different release of Ceph, the list of supported S3
   actions will be different.

In Reef only the following actions are supported:

- ``s3GetObject``
- ``s3GetObjectVersion``
- ``s3PutObject``
- ``s3GetObjectAcl``
- ``s3GetObjectVersionAcl``
- ``s3PutObjectAcl``
- ``s3PutObjectVersionAcl``
- ``s3DeleteObject``
- ``s3DeleteObjectVersion``
- ``s3ListMultipartUploadParts``
- ``s3AbortMultipartUpload``
- ``s3GetObjectTorrent``
- ``s3GetObjectVersionTorrent``
- ``s3RestoreObject``
- ``s3CreateBucket``
- ``s3DeleteBucket``
- ``s3ListBucket``
- ``s3ListBucketVersions``
- ``s3ListAllMyBuckets``
- ``s3ListBucketMultipartUploads``
- ``s3GetAccelerateConfiguration``
- ``s3PutAccelerateConfiguration``
- ``s3GetBucketAcl``
- ``s3PutBucketAcl``
- ``s3GetBucketCORS``
- ``s3PutBucketCORS``
- ``s3GetBucketVersioning``
- ``s3PutBucketVersioning``
- ``s3GetBucketRequestPayment``
- ``s3PutBucketRequestPayment``
- ``s3GetBucketLocation``
- ``s3GetBucketPolicy``
- ``s3DeleteBucketPolicy``
- ``s3PutBucketPolicy``
- ``s3GetBucketNotification``
- ``s3PutBucketNotification``
- ``s3GetBucketLogging``
- ``s3PutBucketLogging``
- ``s3GetBucketTagging``
- ``s3PutBucketTagging``
- ``s3GetBucketWebsite``
- ``s3PutBucketWebsite``
- ``s3DeleteBucketWebsite``
- ``s3GetLifecycleConfiguration``
- ``s3PutLifecycleConfiguration``
- ``s3PutReplicationConfiguration``
- ``s3GetReplicationConfiguration``
- ``s3DeleteReplicationConfiguration``
- ``s3GetObjectTagging``
- ``s3PutObjectTagging``
- ``s3DeleteObjectTagging``
- ``s3GetObjectVersionTagging``
- ``s3PutObjectVersionTagging``
- ``s3DeleteObjectVersionTagging``
- ``s3PutBucketObjectLockConfiguration``
- ``s3GetBucketObjectLockConfiguration``
- ``s3PutObjectRetention``
- ``s3GetObjectRetention``
- ``s3PutObjectLegalHold``
- ``s3GetObjectLegalHold``
- ``s3BypassGovernanceRetention``
- ``s3GetBucketPolicyStatus``
- ``s3PutPublicAccessBlock``
- ``s3GetPublicAccessBlock``
- ``s3DeletePublicAccessBlock``
- ``s3GetBucketPublicAccessBlock``
- ``s3PutBucketPublicAccessBlock``
- ``s3DeleteBucketPublicAccessBlock``
- ``s3GetBucketEncryption``
- ``s3PutBucketEncryption``

We do not yet support setting policies on users, groups, or roles.

We use the RGW ‘tenant’ identifier in place of the Amazon twelve-digit
account ID. In the future we may allow you to assign an account ID to
a tenant, but for now if you want to use policies between AWS S3 and
RGW S3 you will have to use the Amazon account ID as the tenant ID when
creating users.

Under AWS, all tenants share a single namespace. RGW gives every
tenant its own namespace of buckets. There may be an option to enable
an AWS-like 'flat' bucket namespace in future versions. At present, to
access a bucket belonging to another tenant, address it as
"tenant:bucket" in the S3 request.

In AWS, a bucket policy can grant access to another account, and that
account owner can then grant access to individual users with user
permissions. Since we do not yet support user, role, and group
permissions, account owners will currently need to grant access
directly to individual users, and granting an entire account access to
a bucket grants access to all users in that account.

Bucket policies do not yet support string interpolation.

For all requests, condition keys we support are:
- aws:CurrentTime
- aws:EpochTime
- aws:PrincipalType
- aws:Referer
- aws:SecureTransport
- aws:SourceIp
- aws:UserAgent
- aws:username

We support certain s3 condition keys for bucket and object requests.

*Support for the following bucket-related operations was added in the Mimic
release of Ceph.*

Bucket Related Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------------------+----------------------+----------------+
| Permission            | Condition Keys       | Comments       |
+-----------------------+----------------------+----------------+
|                       | s3:x-amz-acl         |                |
|                       | s3:x-amz-grant-<perm>|                |
|s3:createBucket        | where perm is one of |                |
|                       | read/write/read-acp  |                |
|                       | write-acp/           |                |
|                       | full-control         |                |
+-----------------------+----------------------+----------------+
|                       | s3:prefix            |                |
|                       +----------------------+----------------+
| s3:ListBucket &       | s3:delimiter         |                |
|                       +----------------------+----------------+
| s3:ListBucketVersions | s3:max-keys          |                |
+-----------------------+----------------------+----------------+
| s3:PutBucketAcl       | s3:x-amz-acl         |                |
|                       | s3:x-amz-grant-<perm>|                |
+-----------------------+----------------------+----------------+

.. _tag_policy:

Object Related Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------------------------+-----------------------------------------------+-------------------+
|Permission                   |Condition Keys                                 | Comments          |
|                             |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|                             |s3:x-amz-acl & s3:x-amz-grant-<perm>           |                   |
|                             |                                               |                   |
|                             +-----------------------------------------------+-------------------+
|                             |s3:x-amz-copy-source                           |                   |
|                             |                                               |                   |
|                             +-----------------------------------------------+-------------------+
|                             |s3:x-amz-server-side-encryption                |                   |
|                             |                                               |                   |
|                             +-----------------------------------------------+-------------------+
|s3:PutObject                 |s3:x-amz-server-side-encryption-aws-kms-key-id |                   |
|                             |                                               |                   |
|                             +-----------------------------------------------+-------------------+
|                             |s3:x-amz-metadata-directive                    |PUT & COPY to      |
|                             |                                               |overwrite/preserve |
|                             |                                               |metadata in COPY   |
|                             |                                               |requests           |
|                             +-----------------------------------------------+-------------------+
|                             |s3:RequestObjectTag/<tag-key>                  |                   |
|                             |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|s3:PutObjectAcl              |s3:x-amz-acl & s3-amz-grant-<perm>             |                   |
|s3:PutObjectVersionAcl       |                                               |                   |
|                             +-----------------------------------------------+-------------------+
|                             |s3:ExistingObjectTag/<tag-key>                 |                   |
|                             |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|                             |s3:RequestObjectTag/<tag-key>                  |                   |
|s3:PutObjectTagging &        +-----------------------------------------------+-------------------+
|s3:PutObjectVersionTagging   |s3:ExistingObjectTag/<tag-key>                 |                   |
|                             |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|s3:GetObject &               |s3:ExistingObjectTag/<tag-key>                 |                   |
|s3:GetObjectVersion          |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|s3:GetObjectAcl &            |s3:ExistingObjectTag/<tag-key>                 |                   |
|s3:GetObjectVersionAcl       |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|s3:GetObjectTagging &        |s3:ExistingObjectTag/<tag-key>                 |                   |
|s3:GetObjectVersionTagging   |                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+
|s3:DeleteObjectTagging &     |s3:ExistingObjectTag/<tag-key>                 |                   |
|s3:DeleteObjectVersionTagging|                                               |                   |
+-----------------------------+-----------------------------------------------+-------------------+


More may be supported soon as we integrate with the recently rewritten
Authentication/Authorization subsystem.

Swift
=====

There is no way to set bucket policies under Swift, but bucket
policies that have been set govern Swift as well as S3 operations.

Swift credentials are matched against Principals specified in a policy
in a way specific to whatever backend is being used.
