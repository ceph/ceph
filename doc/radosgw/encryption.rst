.. _radosgw-encryption:

==========
Encryption
==========

.. versionadded:: Luminous

The Ceph Object Gateway supports server-side encryption of uploaded objects,
with 3 options for the management of encryption keys. Server-side encryption
means that the data is sent over HTTP in its unencrypted form, and the Ceph
Object Gateway stores that data in the Ceph Storage Cluster in encrypted form.

.. note:: Requests for server-side encryption must be sent over a secure HTTPS
          connection to avoid sending secrets in plaintext. If a proxy is used
          for SSL termination, ``rgw trust forwarded https`` must be enabled
          before forwarded requests will be trusted as secure.

.. note:: Server-side encryption keys must be 256-bit long and base64 encoded.

Customer-Provided Keys
======================

In this mode, the client passes an encryption key along with each request to
read or write encrypted data. It is the client's responsibility to manage those
keys and remember which key was used to encrypt each object.

This is implemented in S3 according to the `Amazon SSE-C`_ specification.

As all key management is handled by the client, no special Ceph configuration
is needed to support this encryption mode.

Key Management Service
======================

In this mode, an administrator stores keys in a secure key management service.
These keys are then
retrieved on demand by the Ceph Object Gateway to serve requests to encrypt
or decrypt data.

This is implemented in S3 according to the `Amazon SSE-KMS`_ specification.

In principle, any key management service could be used here.  Currently
integration with `Barbican`_, `Vault`_, and `KMIP`_ are implemented.

See :ref:`radosgw-barbican`, :ref:`radosgw-vault`,
and :ref:`radosgw-kmip`.

SSE-S3
======

This makes key management invisible to the user.  They are still stored
in Vault, but they are automatically created and deleted by Ceph and
retrieved as required to serve requests to encrypt
or decrypt data.

This is implemented in S3 according to the `Amazon SSE-S3`_ specification.

In principle, any key management service could be used here.  Currently
only integration with `Vault`_, is implemented.

See :ref:`radosgw-vault`.

Bucket Encryption APIs
======================

Bucket Encryption APIs to support server-side encryption with Amazon
S3-managed keys (SSE-S3) or AWS KMS customer master keys (SSE-KMS). 

See `PutBucketEncryption`_, `GetBucketEncryption`_, `DeleteBucketEncryption`_

Automatic Encryption (for testing only)
=======================================

A ``rgw crypt default encryption key`` can be set in ceph.conf to force the
encryption of all objects that do not otherwise specify an encryption mode.

The configuration expects a base64-encoded 256 bit key. For example::

  rgw crypt default encryption key = 4YSmvJtBv0aZ7geVgAsdpRnLBEwWSWlMIGnRS8a9TSA=

.. important:: This mode is for diagnostic purposes only! The ceph configuration
   file is not a secure method for storing encryption keys. Keys that are
   accidentally exposed in this way should be considered compromised.

Caching
=======

The caching feature for Key Management Service (KMS) secrets greatly
improves the performance of server-side encryption and lessens the
load on the KMS.

Secrets are stored using the `Linux Kernel Key Retention Service`_ in
the RGW processes' process keyring. This is subject to a global quota
and must be set in accordance with the configured cache size.
Depending on whether RGW runs as root, these quotas can be managed by adjusting:
- ``/proc/sys/kernel/keys/root_maxkeys`` and ``/proc/sys/kernel/keys/root_maxbytes``
- ``/proc/sys/kernel/keys/maxkeys`` and ``/proc/sys/kernel/keys/maxbytes``

Exceeding a quota will disable the cache, fail the request with an
internal error, and log a failure message.

Three different Cache Time-to-Live (TTL) values can be set:
- **Positive TTL**: How long a successfully retrieved secret remains
  in the cache.
- **Negative TTL**: How long to remember that a key does not exist,
  preventing unnecessary requests to the KMS.
- **Transient Error TTL**: How long to cache failures due to temporary
  issues like KMS timeouts.

Metrics
---------

The cache exports metrics under the ``kms-cache`` collection.
- ``hit``: Hit counter
- ``miss``:  Miss counter
- ``expired``: Number of TTL expired entries
- ``size``: Current cache size
- ``capacity``: Cache maximum size
- ``clear``: Number of cache clears. Resets ``size``, ``hit``,
  ``miss``, ``expired``

In addition the ``rgw`` collection has:
- ``kms_fetch_lat``: Average KMS fetch latency. Also includes a
  successful request counter. Each event results in a positive cache
  entry.
- ``kms_error_transient``: Transient KMS fetch error counter. Each
  event results in a transient error cache entry.
- ``kms_error_permanent``: Permanent KMS fetch error counter. Each
  event results in a negative cache cache entry.

.. _Linux Kernel Key Retention Service:  https://www.kernel.org/doc/html/latest/security/keys/core.html
.. _Amazon SSE-C: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
.. _Amazon SSE-KMS: http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
.. _Amazon SSE-S3: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html
.. _Barbican: https://wiki.openstack.org/wiki/Barbican
.. _Vault: https://www.vaultproject.io/docs/
.. _KMIP: http://www.oasis-open.org/committees/kmip/
.. _PutBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
.. _GetBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
.. _DeleteBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html
