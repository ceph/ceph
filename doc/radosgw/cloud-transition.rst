================
Cloud Transition
================

This feature makes it possible to transition S3 objects to a remote cloud
service as part of the `object lifecycle
<https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html>`_
via :ref:`storage_classes`. The transition is unidirectional: data cannot be
transitioned back from the remote zone. The purpose of this feature is to
enable data transition to multiple cloud providers. Cloud providers compatible
with AWS (S3) are supported.

We use a special storage class of tier type ``cloud-s3`` or
``cloud-s3-glacier`` to configure the remote cloud S3 object store service to
which data is transitioned. These classes are defined in terms of zonegroup
placement targets and, unlike regular storage classes, do not need a data pool.

User credentials for the remote cloud object store service must be
configured. Note that source ACLs will not be preserved. It is possible
to map permissions of specific source users to specific destination users.


Cloud Storage Class Tier Type
-----------------------------

* ``tier-type`` (string)

  The type of remote cloud service that will be used to transition objects.
  The below tier types are supported:

  * ``cloud-s3`` : Regular S3 compatible object store service.

  * ``cloud-s3-glacier`` : S3 Glacier or Tape storage services.


Cloud Storage Class Tier Configuration
--------------------------------------

::

  {
    "access_key": <access>,
    "secret": <secret>,
    "endpoint": <endpoint>,
    "region": <region>,
    "host_style": <path | virtual>,
    "acls": [ { "type": <id | email | uri>,
                "source_id": <source_id>,
                "dest_id": <dest_id> } ... ],
    "target_path": <target_path>,
    "target_storage_class": <target-storage-class>,
    "multipart_sync_threshold": {object_size},
    "multipart_min_part_size": {part_size},
    "retain_head_object": <true | false>
  }


Cloud Transition Specific Configurables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``access_key`` (string)

  The remote cloud S3 access key.

* ``secret`` (string)

  The secret key for the remote cloud S3 service.

* ``endpoint`` (string)

  URL of remote cloud S3 service.

* ``region`` (string)

  The remote cloud S3 service region name.

* ``host_style`` (path | virtual)

  Type of host style to be used when accessing the remote cloud S3 service
  (default: ``path``).

* ``acls`` (array)

  Contains a list of ``acl_mappings``.

* ``acl_mapping`` (container)

  Each ``acl_mapping`` structure contains ``type``, ``source_id``, and
  ``dest_id``. These define the ACL mutation to be done on each object. An ACL
  mutation makes it possible to convert a source userid to a destination
  userid.

* ``type`` (id | email | uri)

  ACL type: ``id`` defines userid, ``email`` defines user by email,
  and ``uri`` defines user by ``uri`` (group).

* ``source_id`` (string)

  ID of user in the source zone.

* ``dest_id`` (string)

  ID of user on the destination.

* ``target_path`` (string)

  A string that defines how the target path is constructed. The target path
  specifies a prefix to which the source bucket-name/object-name is appended.
  If not specified the ``target_path`` created is ``rgwx-${zonegroup}-${storage-class}-cloud-bucket``.

  For example: ``target_path = rgwx-archive-${zonegroup}/``

* ``target_storage_class`` (string)

  A string that defines the target storage class to which the object transitions.
  If not specified, the object is transitioned to the ``STANDARD`` storage class.

* ``retain_head_object`` (true | false)

  If ``true``, the metadata of the object transitioned to the cloud service is retained.
  If ``false`` (default), the object is deleted after the transition.
  This option is ignored for current-versioned objects. For more details,
  refer to the "Versioned Objects" section below.


S3 Specific Configurables
~~~~~~~~~~~~~~~~~~~~~~~~~

Currently, cloud transition will work only with backends that are compatible with
AWS S3 protocol. There are a few configurables that can be used to tweak behavior
when accessing cloud services::

  {
    "multipart_sync_threshold": {object_size},
    "multipart_min_part_size": {part_size}
  }

* ``multipart_sync_threshold`` (integer)

  Objects this size or larger will be transitioned to the cloud using multipart upload.

* ``multipart_min_part_size`` (integer)

  Minimum part size to use when transitioning objects using multipart upload.


How to Configure
~~~~~~~~~~~~~~~~

See :ref:`adding_a_storage_class` for how to configure storage-class for a zonegroup. The cloud transition requires a creation of a special storage class with tier type defined as ``cloud-s3`` or ``cloud-s3-glacier``.

.. note:: If you have not performed previous `Multisite Configuration`_,
          a ``default`` zone and zonegroup are created for you, and changes
          to the zone/zonegroup will not take effect until the Ceph Object
          Gateways (RGW daemons) are restarted. If you have created a realm for multisite,
          the zone/zonegroup changes will take effect once the changes are
          committed with ``radosgw-admin period update --commit``.

.. prompt:: bash #

   radosgw-admin zonegroup placement add --rgw-zonegroup={zone-group-name} \
                                           --placement-id={placement-id} \
                                           --storage-class={storage-class-name} \
                                           --tier-type=cloud-s3 

For example

.. prompt:: bash #

   radosgw-admin zonegroup placement add --rgw-zonegroup=default \
                                           --placement-id=default-placement \
                                           --storage-class=CLOUDTIER --tier-type=cloud-s3

::

  [
      {
          "key": "default-placement",
          "val": {
              "name": "default-placement",
              "tags": [],
              "storage_classes": [
                  "CLOUDTIER",
                  "STANDARD"
              ],
              "tier_targets": [
                  {
                      "key": "CLOUDTIER",
                      "val": {
                          "tier_type": "cloud-s3",
                          "storage_class": "CLOUDTIER",
                          "retain_head_object": "false",
                          "s3": {
                              "endpoint": "",
                              "access_key": "",
                              "secret": "",
                              "host_style": "path",
                              "target_storage_class": "",
                              "target_path": "",
                              "acl_mappings": [],
                              "multipart_sync_threshold": 33554432,
                              "multipart_min_part_size": 33554432
                          }
                      }
                  }
              ]
          }
      }
  ]

.. note:: Once a storage class
          of ``--tier-type=cloud-s3`` or ``--tier-type=cloud-s3-glacier``
          is created it cannot be later modified to any other storage class type.

The tier configuration can be then performed using the following command:

.. prompt:: bash #

   radosgw-admin zonegroup placement modify --rgw-zonegroup={zone-group-name} \
                                              --placement-id={placement-id} \
                                              --storage-class={storage-class-name} \
                                              --tier-config={key}={val}[,{key}={val}]

The ``key`` in the configuration specifies the config variable to be updated, and
the ``val`` specifies its new value.

For example:

.. prompt:: bash #

   radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                              --placement-id default-placement \
                                              --storage-class CLOUDTIER \
                                              --tier-config=endpoint=http://XX.XX.XX.XX:YY,\
                                              access_key=<access_key>,secret=<secret>, \
                                              multipart_sync_threshold=44432, \
                                              multipart_min_part_size=44432, \
                                              retain_head_object=true

Nested tier configuration values can be accessed using periods. This notation
works similarly to how nested fields are accessed in JSON with tools like ``jq``.
Note that the use of period separators ``(.)`` is specific to key access within ``--tier-config``,
and should not be confused with Ceph RGW patterns for realm/zonegroup/zone. 
For example:

.. prompt:: bash #

   radosgw-admin zonegroup placement modify --rgw-zonegroup={zone-group-name} \
                                              --placement-id={placement-id} \
                                              --storage-class={storage-class-name} \
                                              --tier-config=acls.source_id=${source-id}, \
                                              acls.dest_id=${dest-id}

Configuration array entries can be accessed by specifying the specific entry to
be referenced enclosed in square brackets, and adding a new array entry can be
performed with an empty array `[]`.
For example, creating a new ``acl`` array entry:

.. prompt:: bash #

   radosgw-admin zonegroup placement modify --rgw-zonegroup={zone-group-name} \
                                              --placement-id={placement-id} \
                                              --storage-class={storage-class-name} \
                                              --tier-config=acls[].source_id=${source-id}, \
                                              acls[${source-id}].dest_id=${dest-id}, \
                                              acls[${source-id}].type=email

An entry can be removed by supplying ``--tier-config-rm={key}``.

For example:

.. prompt:: bash #

   radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                              --placement-id default-placement \
                                              --storage-class CLOUDTIER \
                                              --tier-config-rm=acls.source_id=testid
   radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                              --placement-id default-placement \
                                              --storage-class CLOUDTIER \
                                              --tier-config-rm=target_path

The storage class can be removed using the following command:

.. prompt:: bash #

   radosgw-admin zonegroup placement rm --rgw-zonegroup={zone-group-name} \
                                          --placement-id={placement-id} \
                                          --storage-class={storage-class-name}

For example:

.. prompt:: bash #

   radosgw-admin zonegroup placement rm --rgw-zonegroup default \
                                          --placement-id default-placement \
                                          --storage-class CLOUDTIER

::

  [
      {
          "key": "default-placement",
          "val": {
              "name": "default-placement",
              "tags": [],
              "storage_classes": [
                  "STANDARD"
              ]
          }
      }
  ]


Object Modification and Limitations
-----------------------------------

The cloud storage class, once configured, can be used like any other storage
class when defining bucket lifecycle (LC) rules. For example::

  <LifecycleConfiguration>
    <Rule>
      ....
      <Transition>
        ....
        <StorageClass>CLOUDTIER</StorageClass>
      </Transition>
    </Rule>
  </LifecycleConfiguration>

Since the transition is unidirectional, when configuring S3
lifecycle rules, the cloud storage class should be specified
last among all the storage classes the object transitions to.
Subsequent rules (if any) do not apply post-transition to the cloud.

Due to API limitations, there is no way to preserve the original object
modification time and ETag, which are stored as metadata attributes
on the destination objects, as shown below::

  x-amz-meta-rgwx-source: rgw
  x-amz-meta-rgwx-source-etag: ed076287532e86365e841e92bfc50d8c
  x-amz-meta-rgwx-source-key: lc.txt
  x-amz-meta-rgwx-source-mtime: 1608546349.757100363
  x-amz-meta-rgwx-versioned-epoch: 0

In order to allow cloud services to detect the source and map
user-defined ``x-amz-meta-`` attributes, two additional new
attributes are added to the objects being transitioned:

* ``x-rgw-cloud`` : ``true``/``false``

   ``true``, by default, if the object is being transitioned from RGW.

* ``x-rgw-cloud-keep-attrs`` : ``true`` / ``false``

   If set to default ``true``, the cloud service should map and store all
   the ``x-amz-meta-*`` attributes. If it cannot, then the operation should fail.

   If set to ``false``, the cloud service can ignore such attributes and
   just store the object data being sent.

By default, post-transition, the source object gets deleted. But it is possible
to retain its metadata with updated values (including ``storage-class``
and ``object-size``) by setting the config option ``retain_head_object``
to true. However a ``GET`` operation on such an object will still fail
with an ``InvalidObjectState`` error. Any other operations against original
source objects will be for its metadata entries only keeping transitioned
objects intact.

For example:

.. prompt:: bash $

   s3cmd info s3://bucket/lc.txt

::

  s3://bucket/lc.txt (object):
     File size: 12
     Last mod:  Mon, 21 Dec 2020 10:25:56 GMT
     MIME type: text/plain
     Storage:   CLOUDTIER
     MD5 sum:   ed076287532e86365e841e92bfc50d8c
     SSE:       none
     Policy:    none
     CORS:      none
     ACL:       M. Tester: FULL_CONTROL
     x-amz-meta-s3cmd-attrs: atime:1608466266/ctime:1597606156/gid:0/gname:root/md5:ed076287532e86365e841e92bfc50d8c/mode:33188/mtime:1597605793/uid:0/uname:root
  
.. prompt:: bash $

   s3cmd get s3://bucket/lc.txt lc_restore.txt

::

  download: 's3://bucket/lc.txt' -> 'lc_restore.txt'  [1 of 1]
  ERROR: S3 error: 403 (InvalidObjectState)

To avoid object name collisions across buckets, the source bucket name is
prepended to the target object name. If the object is versioned, the object's
``versionid`` is appended.

Below is the object name format::

  s3://<target_path>/<source_bucket_name>/<source_object_name>(-<source_object_version_id>)


Versioned Objects
~~~~~~~~~~~~~~~~~

For versioned and locked objects, similar semantics as that of LifecycleExpiration are applied as stated below.

* If the object is current, post transitioning to cloud, it is made noncurrent with delete marker created.

* If the object is noncurrent and is locked, its transition is skipped.


Restoring Objects
-----------------
The objects transitioned to cloud can now be restored. For more information, refer to
`Restoring Objects from Cloud <https://docs.ceph.com/en/latest/radosgw/cloud-restore/>`_.


Future Work
-----------

* Send presigned redirect or read-through the objects transitioned to cloud.

* Support transition to other cloud providers (like Azure).

.. _`Multisite Configuration`: ../multisite
