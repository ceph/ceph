.. _radosgw-s3control:

==========
S3 Control
==========

.. versionadded:: Umbrella

The Ceph Object Gateway supports a subset of the `AWS S3 Control API`_ for
``PublicAccessBlock`` configuration on :ref:`User Accounts <radosgw-account>`,
similar to the S3 API's ``PublicAccessBlock`` configuration for buckets. This
account-level configuration applies to all buckets owned by the account. When
configured at both account- and bucket-level, precedence goes to whichever is
more restrictive.

Configuration
-------------

Support for this feature is controlled by :confval:`rgw_enable_apis`, where
``s3control`` is enabled by default.

Wildcard DNS
~~~~~~~~~~~~

When issuing S3 Control requests, `awscli`_ and some AWS SDKs add the account
id to the hostname like ``rgw69573912842483864.s3.example.com``, so expect a
DNS configuration that allows a wildcard subdomain. If the
:ref:`HTTP Frontend <rgw_frontends>` is configured for SSL, its certificate
must also match this wildcard subdomain.

Because this behavior is not documented in the AWS API reference (and the
account id is already supplied in the ``x-amz-account-id`` request header),
Ceph Object Gateway does not require the account to be part of the hostname
and will ignore it when given.

Operations
----------

The following table describes the currently supported S3 Control actions.

+------------------------------+---------------------------------------------+
| Action                       | Remarks                                     |
+==============================+=============================================+
| **PutPublicAccessBlock**     |                                             |
+------------------------------+---------------------------------------------+
| **GetPublicAccessBlock**     |                                             |
+------------------------------+---------------------------------------------+
| **DeletePublicAccessBlock**  |                                             |
+------------------------------+---------------------------------------------+

.. _AWS S3 Control API: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_AWS_S3_Control.html
.. _awscli: https://aws.amazon.com/cli/
