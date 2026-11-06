.. _radosgw-migration:

==================================
Migrating Objects from S3 Services
==================================

.. versionadded:: Vampire

.. contents::

The S3 API does not allow clients to set an object's modification time
(mtime) or version id, so objects copied from another S3 service get the
current time and new version ids. This breaks age-based lifecycle rules and
applications that store version ids.

RGW lets users with the ``migration`` capability keep the original mtime and
version id on upload.


Granting the Capability
=======================

.. prompt:: bash #

   radosgw-admin caps add --uid={uid} --caps="migration=write"

The capability grants no other permissions.


Request Parameters
==================

Users with the ``migration=write`` capability can set the object's last
modified time and version id with two query parameters on ``PutObject`` and
``CompleteMultipartUpload`` requests:

::

    PUT /{bucket}/{object}?rgwx-migration-mtime={mtime}&rgwx-migration-version-id={version-id} HTTP/1.1
    POST /{bucket}/{object}?uploadId={upload-id}&rgwx-migration-mtime={mtime}&rgwx-migration-version-id={version-id} HTTP/1.1

- ``rgwx-migration-mtime``: Unix time in seconds, with an optional fraction.
  For example: ``1600000000`` or ``1600000000.5``.
- ``rgwx-migration-version-id``: the version id. Ignored if versioning is not
  enabled on the bucket.

The parameters are ignored on other requests, or if the user does not have
the capability. Otherwise, the request fails only with:

- ``400 InvalidArgument``: invalid ``rgwx-migration-mtime`` value.
- ``409 InvalidBucketState``: ``rgwx-migration-mtime`` on a bucket with object
  lock enabled. Overriding the mtime is not allowed there, because it can
  shorten retention periods.

To migrate object versions, enable versioning on the target bucket and upload
the versions from oldest to newest.


Example
=======

Adding the parameters with boto3:

.. code-block:: python

   import urllib.parse
   import boto3

   client = boto3.client('s3', endpoint_url='http://localhost:8000',
                         aws_access_key_id='access_key',
                         aws_secret_access_key='secret_key')

   def add_params(request, **kwargs):
       request.url += '?' + urllib.parse.urlencode({
           'rgwx-migration-mtime': '1600000000.5',
           'rgwx-migration-version-id': 'source-version-id',
       })

   client.meta.events.register('before-sign.s3.PutObject', add_params)
   client.put_object(Bucket='bucket', Key='key', Body=b'data')

Alternatively, the RGW `extensions file`_ adds the parameters to boto3 and the
AWS CLI as ``MigrationMtime`` and ``MigrationVersionId``. See
:ref:`radosgw-s3-python-extensions`.

.. _extensions file: https://github.com/ceph/ceph/blob/main/examples/rgw/boto3/service-2.sdk-extras.json
