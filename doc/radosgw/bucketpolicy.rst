===============
Bucket Policies
===============

The Ceph Object Gateway supports a subset of the Amazon S3 policy
language applied to buckets.


Creation and Removal
====================

Bucket policies are managed through standard S3 operations rather than
``radosgw-admin``.

For example, one may use ``s3cmd`` to set or delete a policy thus::

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

See :ref:`Principals <radosgw-account-principals>` regarding
the interactions between policy Principals and User Accounts.

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

Request that authenticate with Keystone also include:

- keystone:role
- keystone:userid

We support certain S3 condition keys for bucket and object requests.

*Support for the following bucket-related operations was added in the Mimic
release of Ceph.*

Bucket Related Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------------------+----------------------+----------------+
| Permission            | Condition Keys       | Comments       |
+-----------------------+----------------------+----------------+
|                       | s3:x-amz-acl         |                |
|                       | s3:x-amz-grant-<perm>|                |
|s3:CreateBucket        | where perm is one of |                |
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

+-----------------------------+---------------------------------------------------+-------------------+
|Permission                   |Condition Keys                                     | Comments          |
|                             |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|                             |s3:x-amz-acl & s3:x-amz-grant-<perm>               |                   |
|                             |                                                   |                   |
|                             +---------------------------------------------------+-------------------+
|                             |s3:x-amz-copy-source                               |                   |
|                             |                                                   |                   |
|                             +---------------------------------------------------+-------------------+
|                             |s3:x-amz-server-side-encryption                    |                   |
|                             |                                                   |                   |
|                             +---------------------------------------------------+-------------------+
|s3:PutObject                 |s3:x-amz-server-side-encryption-aws-kms-key-id     |                   |
|                             |                                                   |                   |
|                             +---------------------------------------------------+-------------------+
|                             |s3:x-amz-server-side-encryption-customer-algorithm |                   |
|                             |                                                   |                   |
|                             +---------------------------------------------------+-------------------+
|                             |s3:x-amz-metadata-directive                        |PUT & COPY to      |
|                             |                                                   |overwrite/preserve |
|                             |                                                   |metadata in COPY   |
|                             |                                                   |requests           |
|                             +---------------------------------------------------+-------------------+
|                             |s3:RequestObjectTag/<tag-key>                      |                   |
|                             |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|s3:PutObjectAcl              |s3:x-amz-acl & s3:x-amz-grant-<perm>               |                   |
|s3:PutObjectVersionAcl       |                                                   |                   |
|                             +---------------------------------------------------+-------------------+
|                             |s3:ExistingObjectTag/<tag-key>                     |                   |
|                             |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|                             |s3:RequestObjectTag/<tag-key>                      |                   |
|s3:PutObjectTagging &        +---------------------------------------------------+-------------------+
|s3:PutObjectVersionTagging   |s3:ExistingObjectTag/<tag-key>                     |                   |
|                             |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|s3:GetObject &               |s3:ExistingObjectTag/<tag-key>                     |                   |
|s3:GetObjectVersion          |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|s3:GetObjectAcl &            |s3:ExistingObjectTag/<tag-key>                     |                   |
|s3:GetObjectVersionAcl       |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|s3:GetObjectTagging &        |s3:ExistingObjectTag/<tag-key>                     |                   |
|s3:GetObjectVersionTagging   |                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+
|s3:DeleteObjectTagging &     |s3:ExistingObjectTag/<tag-key>                     |                   |
|s3:DeleteObjectVersionTagging|                                                   |                   |
+-----------------------------+---------------------------------------------------+-------------------+


Swift
=====

There is no way to set bucket policies under Swift, but bucket
policies that have been set govern Swift as well as S3 operations.

Swift credentials are matched against Principals specified in a policy
in a way specific to whatever backend is being used.
