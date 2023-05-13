================
Cloud Transition
================

This feature enables data transition to a remote cloud service as part of `Lifecycle Configuration <https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html>`__ via :ref:`storage_classes`. The transition is unidirectional; data cannot be transitioned back from the remote zone. The goal of this feature is to enable data transition to multiple cloud providers. The currently supported cloud providers are those that are compatible with AWS (S3).

Special storage class of tier type ``cloud-s3`` is used to configure the remote cloud S3 object store service to which the data needs to be transitioned. These are defined in terms of zonegroup placement targets and unlike regular storage classes, do not need a data pool.

User credentials for the remote cloud object store service need to be configured. Note that source ACLs will not
be preserved. It is possible to map permissions of specific source users to specific destination users.


Cloud Storage Class Configuration
---------------------------------

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


Cloud Transition Specific Configurables:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``access_key`` (string)

The remote cloud S3 access key that will be used for a specific connection.

* ``secret`` (string)

The secret key for the remote cloud S3 service.

* ``endpoint`` (string)

URL of remote cloud S3 service endpoint.

* ``region`` (string)

The remote cloud S3 service region name.

* ``host_style`` (path | virtual)

Type of host style to be used when accessing remote cloud S3 endpoint (default: ``path``).

* ``acls`` (array)

Contains a list of ``acl_mappings``.

* ``acl_mapping`` (container)

Each ``acl_mapping`` structure contains ``type``, ``source_id``, and ``dest_id``. These
will define the ACL mutation that will be done on each object. An ACL mutation allows converting source
user id to a destination id.

* ``type`` (id | email | uri)

ACL type: ``id`` defines user id, ``email`` defines user by email, and ``uri`` defines user by ``uri`` (group).

* ``source_id`` (string)

ID of user in the source zone.

* ``dest_id`` (string)

ID of user in the destination.

* ``target_path`` (string)

A string that defines how the target path is created. The target path specifies a prefix to which
the source 'bucket-name/object-name' is appended. If not specified the target_path created is "rgwx-${zonegroup}-${storage-class}-cloud-bucket".

For example: ``target_path = rgwx-archive-${zonegroup}/``

* ``target_storage_class`` (string)

A string that defines the target storage class to which the object transitions to. If not specified, object is transitioned to STANDARD storage class.

* ``retain_head_object`` (true | false)

If true, retains the metadata of the object transitioned to cloud. If false (default), the object is deleted post transition.
This option is ignored for current versioned objects. For more details, refer to section "Versioned Objects" below.


S3 Specific Configurables:
~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently cloud transition will only work with backends that are compatible with AWS S3. There are
a few configurables that can be used to tweak its behavior when accessing these cloud services:

::

    {
      "multipart_sync_threshold": {object_size},
      "multipart_min_part_size": {part_size}
    }


* ``multipart_sync_threshold`` (integer)

Objects this size or larger will be transitioned to the cloud using multipart upload.

* ``multipart_min_part_size`` (integer)

Minimum parts size to use when transitioning objects using multipart upload.


How to Configure
~~~~~~~~~~~~~~~~

See :ref:`adding_a_storage_class` for how to configure storage-class for a zonegroup. The cloud transition requires a creation of a special storage class with tier type defined as ``cloud-s3``

.. note:: If you have not done any previous `Multisite Configuration`_,
          a ``default`` zone and zonegroup are created for you, and changes
          to the zone/zonegroup will not take effect until the Ceph Object
          Gateways are restarted. If you have created a realm for multisite,
          the zone/zonegroup changes will take effect once the changes are
          committed with ``radosgw-admin period update --commit``.

::

    # radosgw-admin zonegroup placement add --rgw-zonegroup={zone-group-name} \
                                            --placement-id={placement-id} \
                                            --storage-class={storage-class-name} \
                                            --tier-type=cloud-s3 

For example:

::

    # radosgw-admin zonegroup placement add --rgw-zonegroup=default \
                                            --placement-id=default-placement \
                                            --storage-class=CLOUDTIER --tier-type=cloud-s3
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


.. note:: Once a storage class is created of ``--tier-type=cloud-s3``, it cannot be later modified to any other storage class type.

The tier configuration can be then done using the following command

::

    # radosgw-admin zonegroup placement modify --rgw-zonegroup={zone-group-name} \
                                               --placement-id={placement-id} \
                                               --storage-class={storage-class-name} \
                                               --tier-config={key}={val}[,{key}={val}]

The ``key`` in the configuration specifies the config variable that needs to be updated, and
the ``val`` specifies its new value.


For example:

::

    # radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                               --placement-id default-placement \
                                               --storage-class CLOUDTIER \
                                               --tier-config=endpoint=http://XX.XX.XX.XX:YY,\
                                               access_key=<access_key>,secret=<secret>, \
                                               multipart_sync_threshold=44432, \
                                               multipart_min_part_size=44432, \
                                               retain_head_object=true

Nested values can be accessed using period. For example:

::

    # radosgw-admin zonegroup placement modify --rgw-zonegroup={zone-group-name} \
                                               --placement-id={placement-id} \
                                               --storage-class={storage-class-name} \
                                               --tier-config=acls.source_id=${source-id}, \
                                               acls.dest_id=${dest-id}



Configuration array entries can be accessed by specifying the specific entry to be referenced enclosed
in square brackets, and adding new array entry can be done by using `[]`.
For example, creating a new acl array entry:

::

    # radosgw-admin zonegroup placement modify --rgw-zonegroup={zone-group-name} \
                                               --placement-id={placement-id} \
                                               --storage-class={storage-class-name} \
                                               --tier-config=acls[].source_id=${source-id}, \
                                               acls[${source-id}].dest_id=${dest-id}, \
                                               acls[${source-id}].type=email

An entry can be removed by using ``--tier-config-rm={key}``.

For example,

::

    # radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                               --placement-id default-placement \
                                               --storage-class CLOUDTIER \
                                               --tier-config-rm=acls.source_id=testid

    # radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                               --placement-id default-placement \
                                               --storage-class CLOUDTIER \
                                               --tier-config-rm=target_path

The storage class can be removed using the following command

::

    # radosgw-admin zonegroup placement rm --rgw-zonegroup={zone-group-name} \
                                           --placement-id={placement-id} \
                                           --storage-class={storage-class-name}

For example,

::

    # radosgw-admin zonegroup placement rm --rgw-zonegroup default \
                                           --placement-id default-placement \
                                           --storage-class CLOUDTIER
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

Object modification & Limitations
----------------------------------

The cloud storage class once configured can then be used like any other storage class in the bucket lifecycle rules. For example,

::

    <Transition>
      <StorageClass>CLOUDTIER</StorageClass>
      ....
      ....
    </Transition>


Since the transition is unidirectional, while configuring S3 lifecycle rules, the cloud storage class should be specified last among all the storage classes the object transitions to. Subsequent rules (if any) do not apply post transition to the cloud.

Due to API limitations there is no way to preserve original object modification time and ETag but they get stored as metadata attributes on the destination objects, as shown below:

::

   x-amz-meta-rgwx-source: rgw
   x-amz-meta-rgwx-source-etag: ed076287532e86365e841e92bfc50d8c
   x-amz-meta-rgwx-source-key: lc.txt
   x-amz-meta-rgwx-source-mtime: 1608546349.757100363
   x-amz-meta-rgwx-versioned-epoch: 0

In order to allow some cloud services detect the source and map the user-defined 'x-amz-meta-' attributes, below two additional new attributes are added to the objects being transitioned 

::
    
   x-rgw-cloud : true/false
   (set to "true", by default, if the object is being transitioned from RGW)

   x-rgw-cloud-keep-attrs : true/false
   (if set to default value "true", the cloud service should map and store all the x-amz-meta-* attributes. If it cannot, then the operation should fail.
    if set to "false", the cloud service can ignore such attributes and just store the object data being sent.)


By default, post transition, the source object gets deleted. But it is possible to retain its metadata but with updated values (like storage-class and object-size) by setting config option 'retain_head_object' to true. However GET on those objects shall still fail with 'InvalidObjectState' error.

For example,
::

    # s3cmd info s3://bucket/lc.txt
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

    # s3cmd get s3://bucket/lc.txt lc_restore.txt
    download: 's3://bucket/lc.txt' -> 'lc_restore.txt'  [1 of 1]
    ERROR: S3 error: 403 (InvalidObjectState)

To avoid object names collision across various buckets, source bucket name is prepended to the target object name. If the object is versioned, object versionid is appended to the end.

Below is the sample object name format:
::

    s3://<target_path>/<source_bucket_name>/<source_object_name>(-<source_object_version_id>)


Versioned Objects
~~~~~~~~~~~~~~~~~

For versioned and locked objects, similar semantics as that of LifecycleExpiration are applied as stated below.

* If the object is current, post transitioning to cloud, it is made noncurrent with delete marker created.

* If the object is noncurrent and is locked, its transition is skipped.


Future Work
-----------

* Send presigned redirect or read-through the objects transitioned to cloud

* Support s3:RestoreObject operation on cloud transitioned objects.

* Federation between RGW and Cloud services.

* Support transition to other cloud providers (like Azure).

.. _`Multisite Configuration`: ../multisite
