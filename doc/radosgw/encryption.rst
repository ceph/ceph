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

As all key management is handled by the client, no special configuration is
needed to support this encryption mode.

Key Management Service
======================

This mode allows keys to be stored in a secure key management service and
retrieved on demand by the Ceph Object Gateway to serve requests to encrypt
or decrypt data.

This is implemented in S3 according to the `Amazon SSE-KMS`_ specification.

In principle, any key management service could be used here.  Currently
integration with `Barbican`_, `Vault`_, and `KMIP`_ are implemented.

See `OpenStack Barbican Integration`_, `HashiCorp Vault Integration`_,
and `KMIP Integration`_.

Bucket Encryption Subresource
=============================

Amazon S3 supports APIs to manage the encryption subresource to
configure default encryption. Default encryption for a bucket can use
server-side encryption with Amazon S3-managed keys (SSE-S3) or AWS KMS customer
master keys (SSE-KMS). SSE-KMS implementation via BucketEncryption APIs is not
supported yet.

Details for 3 APIs can be found here: `PutBucketEncryption`_,
`GetBucketEncryption`_, and `DeleteBucketEncryption`_

PutBucketEncryption
-------------------

Places encryption configuration on the bucket. Rules specified in the Server
Side Encryption Configuration will be applied to every new object placed inside
the bucket unless otherwise specific headers are passed to bypass this.

Syntax
~~~~~~

::

    PUT /?encryption HTTP/1.1

Request Entities
~~~~~~~~~~~~~~~~

+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+
| Name                                   | Type        | Description                                                                                           | Required |
+========================================+=============+=======================================================================================================+==========+
| ``ServerSideEncryptionConfiguration``  | Container   | A container for the request.                                                                          |   Yes    |
+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+
| ``Rule``                               | Container   | The Encryption rule in place for the specified bucket.                                                |   No     |
+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+
| ``ApplyServerSideEncryptionByDefault`` | Container   | Apply this by default if put object call doesn't specify encryption.                                  |   Yes    |
+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+
| ``SSEAlgorithm``                       | String      | Server-side encryption algorithm to use for the default encryption.                                   |   Yes    |
+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+
| ``KMSMasterKeyID``                     | String      | AWS KMS key ID to use for the default encryption. Allow this only when SSEAlgorithm is set to aws:kms |   No     |
+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+
| ``BucketKeyEnabled``                   | Boolean     | If enabled it forces use of S3 Bucket Key using SSE-KMS.                                              |   No     |
+----------------------------------------+-------------+-------------------------------------------------------------------------------------------------------+----------+

HTTP Response
~~~~~~~~~~~~~

+---------------+-----------------------+--------------------------------------------------------------------------------------------+
| HTTP Status   | Status Code           | Description                                                                                |
+===============+=======================+============================================================================================+
| ``400``       | MalformedXML          | The XML is not well-formed / SSEAlgorithm other than AES256 / KMSMasterKeyID field is sent |
+---------------+-----------------------+--------------------------------------------------------------------------------------------+

GetBucketEncryption
-------------------

Gets the encryption configuration present for the bucket. Key Id is not displayed to the user as it is not to be managed by a user.

Syntax
~~~~~~

::

    GET /?encryption HTTP/1.1

HTTP Response
~~~~~~~~~~~~~

+---------------+------------------------------------------------+----------------------------------------------------------+
| HTTP Status   | Status Code                                    | Description                                              |
+===============+================================================+==========================================================+
| ``400``       | ServerSideEncryptionConfigurationNotFoundError | When encryption config not present for the bucket        |
+---------------+------------------------------------------------+----------------------------------------------------------+

DeleteBucketEncryption
----------------------

Deletes the encryption configuration present for the bucket.

Syntax
~~~~~~

::

    DELETE /?encryption HTTP/1.1

HTTP Response
~~~~~~~~~~~~~

+---------------+--------------+-----------------------------------------------------------------------------------------------------+
| HTTP Status   | Status Code  | Description                                                                                         |
+===============+==============+=====================================================================================================+
| ``204``       | NoContent    | Bucket encryption config is present and successfully removed / Bukcet encryption config not present |
+---------------+--------------+-----------------------------------------------------------------------------------------------------+

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
.. _Barbican: https://wiki.openstack.org/wiki/Barbican
.. _Vault: https://www.vaultproject.io/docs/
.. _KMIP: http://www.oasis-open.org/committees/kmip/
.. _PutBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
.. _GetBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
.. _DeleteBucketEncryption: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html
.. _OpenStack Barbican Integration: ../barbican
.. _HashiCorp Vault Integration: ../vault
.. _KMIP Integration: ../kmip
