==========
Encryption
==========

.. versionadded:: Luminous

The Ceph Object Gateway supports server-side encryption of uploaded objects,
with 3 options for the management of encryption keys. Server-side encryption
means that the data is sent over HTTP in its unencrypted form, and the Ceph
Object Gateway stores that data in the Ceph Storage Cluster in encrypted form.

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

In principle, any key management service could be used here, but currently
only integration with `Barbican`_ is implemented.

See `OpenStack Barbican Integration`_.

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
.. _OpenStack Barbican Integration: ../barbican
