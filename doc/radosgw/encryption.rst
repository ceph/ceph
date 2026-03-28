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

Encryption Algorithm
====================

.. versionadded:: Umbrella

The Ceph Object Gateway supports two AES-256 encryption algorithms for
server-side encryption:

**AES-256-CBC** (Cipher Block Chaining)
  The legacy encryption algorithm. This mode is compatible with older Ceph
  releases and is the default for backward compatibility. CBC mode encrypts
  data but does not provide built-in integrity verification.

**AES-256-GCM** (Galois/Counter Mode)
  A modern authenticated encryption algorithm that provides both
  confidentiality and integrity protection. GCM mode detects any tampering
  or corruption of encrypted data. This is the recommended algorithm for
  new deployments.

The encryption algorithm for new objects can be configured with::

  rgw crypt sse algorithm = aes-256-cbc    # default, for backward compatibility
  rgw crypt sse algorithm = aes-256-gcm    # recommended for new deployments

.. note:: This setting only affects newly encrypted objects. Existing objects
          are always decrypted using the algorithm that was used when they
          were encrypted, regardless of the current setting. This allows
          CBC-encrypted and GCM-encrypted objects to coexist in the same
          cluster.

.. important:: When upgrading from an older Ceph release, keep the default
               ``aes-256-cbc`` setting until all RGW instances have been
               upgraded. Once all instances support GCM, you can enable
               ``aes-256-gcm`` for new uploads.

GCM Encryption Format
---------------------

AES-256-GCM encrypts data in 4 KB (4096 byte) chunks. Each chunk produces
4112 bytes of ciphertext (4096 bytes of encrypted data plus a 16-byte
authentication tag).

.. list-table:: GCM Size Calculation Example
   :header-rows: 1
   :widths: 40 30 30

   * - Description
     - Size
     - Notes
   * - Original plaintext
     - 10,000 bytes
     - User's data
   * - Number of chunks
     - 3
     - ⌈10000 ÷ 4096⌉
   * - Authentication tags
     - 48 bytes
     - 3 chunks × 16 bytes
   * - **Encrypted on disk**
     - **10,048 bytes**
     - plaintext + tags

The storage overhead is approximately 0.4% (16 bytes per 4 KB). S3 API
responses always report the original plaintext size, so this overhead is
transparent to clients.

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


.. _Amazon SSE-C: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
.. _Amazon SSE-KMS: http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
.. _Amazon SSE-S3: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html
.. _Barbican: https://wiki.openstack.org/wiki/Barbican
.. _Vault: https://www.vaultproject.io/docs/
.. _KMIP: http://www.oasis-open.org/committees/kmip/
.. _PutBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
.. _GetBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
.. _DeleteBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html
