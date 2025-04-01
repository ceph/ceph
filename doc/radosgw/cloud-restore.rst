=============
Cloud Restore
=============

`cloud-transition <https://docs.ceph.com/en/latest/radosgw/cloud-transition>`__   feature enables data transition to a remote cloud service. The ``cloud-restore`` feature enables restoration of those transitioned objects from the remote cloud S3 endpoints into RGW.

This feature currently enables restore of objects that are transitioned to only S3 compatible cloud services. In order to validate this, ``retain_head_object`` option should be set to true in the ``tier-config`` while configuring storage class.

The objects can be restored using `S3 RestoreObject <https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html>`__ API . The restored copies will be retained on RGW only for the duration of ``Days`` specified. However if ``Days`` are not provided, the downloaded copy is considered permanent and will be treated as regular object.
In addition, by enabling ``allow_read_through`` option, `S3 GetObject <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>`__ API can be used as well to restore the object temporarily.


Cloud Storage Class Tier Configuration
--------------------------------------

The `tier configuration <https://docs.ceph.com/en/latest/radosgw/cloud-transition/#cloud-storage-class-configuration>`__ of the cloud storage class configured for data transition is used to restore objects as well.

::

    {
      "access_key": <access>,
      "secret": <secret>,`
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

Additionally, below options have been added to the tier configuration to facilitate object restoration.

* ``restore_storage_class`` (string)

The storage class to which the object data needs to be restored to. Default value is `STANDARD`.


read-through specific Configurables:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``allow_read_through`` (true | false)

If true, enables ``read-through``. Objects can then be restored using ``S3 GetObject`` API as well.

* ``read_through_restore_days`` (interger)

The duration for which objects restored via ``read-through`` are retained for. Default value is 1 day.

For example:

::

    # radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                               --placement-id default-placement \
                                               --storage-class CLOUDTIER \
                                               --tier-config=endpoint=http://XX.XX.XX.XX:YY,\
                                               access_key=<access_key>,secret=<secret>, \
                                               retain_head_object=true, \
                                               restore_storage_class=COLDTIER, \
                                               allow_read_through=true, \
                                               read_through_restore_days=10



S3 Glacier specific Configurables:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To restore objects archived in an S3 Glacier or Tape cloud storage, the data must first be restored to the cloud service before being read and downloaded into RGW. To enable this process, ensure the storage class is configured with ``--tier-type=cloud-s3-glacier``. Additionally, the following configurables should be set accordingly:

* ``glacier_restore_days`` (integer)

The duration of the objects to be restored on the remote cloud service.

* ``glacier_restore_tier_type`` (Standard | Expedited)

The type of retrieval of the object archived on the cloud service. Supported options are ``Standard`` and ``Expedited``.


For example:

::

    # radosgw-admin zonegroup placement add --rgw-zonegroup=default \
                                            --placement-id=default-placement \
                                            --storage-class=CLOUDTIER-GLACIER --tier-type=cloud-s3-glacier

    # radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                               --placement-id default-placement \
                                               --storage-class CLOUDTIER \
                                               --tier-config=endpoint=http://XX.XX.XX.XX:YY,\
                                               access_key=XXXXX,secret=YYYYY, \
                                               retain_head_object=true, \
                                               target_storage_class=Glacier, \
                                               ............
                                               ............
                                               restore_storage_class=COLDTIER, \
                                               glacier_restore_days=2, \
                                               glacier_restore_tier_type=Expedited


     [
        {
            "key": "default-placement",
            "val": {
                "name": "default-placement",
                "tags": [],
                "storage_classes": [
                    "CLOUDTIER-GLACIER",
                    "STANDARD"
                ],
                "tier_targets": [
                    {
                        "key": "CLOUDTIER-GLACIER",
                        "val": {
                            "tier_type": "cloud-s3-glacier",
                            "storage_class": "CLOUDTIER-GLACIER",
                            "retain_head_object": "true",
                            "s3": {
                                "endpoint": http://XX.XX.XX.XX:YY,
                                "access_key": "XXXXX",
                                "secret": "YYYYY",
                                "host_style": "path",
                                "target_storage_class": "Glacier",
                                .......
                                .......
                            }
                            "allow_read_through": true,
                            "read_through_restore_days": 10,
                            "restore_storage_class": "COLDTIER",
                            "s3-glacier": {
                                "glacier_restore_days": 2
                                "glacier_restore_tier_type": "Expedited"
                            }
                        }
                    }
                ]
            }
        }
    ]



Examples to Restore Object:
---------------------------

Using S3 RestoreObject CLI
~~~~~~~~~~~~~~~~~~~~~~~~~

Below options of `S3 restore-object <https://docs.aws.amazon.com/cli/latest/reference/s3api/restore-object.html>`__ CLI are supported  -


Syntax

::


  $ aws s3api restore-object 
            --bucket <value>
            --key <value>
            [--version-id <value>]
            --restore-request (structure) {
              Days=<integer>
            }

Note: ``Days`` is optional and if not provided, the object is restored permanently

Example 1:

::

  $ aws s3api restore-object  --bucket bucket1 --key doc1.rtf 
                              [--version-id 3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo]
                              --restore-request Days=10 
                              ....

This will restore the object `doc1.rtf` of the given version for the duration of 10 days.



Example 2:

::

  $ aws s3api restore-object  --bucket bucket1 --key doc2.rtf --restore-request {} ....

This will restore the object `doc2.rtf` permanently and will be treated as regular object.



Using S3 GetObject CLI
~~~~~~~~~~~~~~~~~~~~~
Ensure ``allow_read_through`` tier-config option is enabled.

Example 3:

::

  $ aws s3api get-object  --bucket bucket1 --key doc3.rtf ....

This will restore the object `doc3.rtf` for the duration of the ``read_through_restore_days`` configured.


Note: The above CLI command may time out if the object restoration takes too long. Before reissuing the command, you can verify the restoration status.


Verifying the restore status
----------------------------
Verify the status of the restore by running an `S3 HeadObject <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax>`__  request. The response includes ``x-amz-restore`` header if either the object restoration is in progress or a copy of it is already restored.

Example,

::

  $ aws s3api head-object --key doc1.rtf --bucket bucket1 ....


In addition, ``radosgw-admin`` CLI can be used to check the restoration status and other details on the RGW server.

Example,

::
  
 $ radosgw-admin object stat --bucket bucket1 --object doc1.rtf


Restored Object Properties
--------------------------


Storage
~~~~~~
The objects are restored to the storage class configured for ``restore_storage_class`` tier-config option. However, as per `AWS S3 RestoreObject <https://docs.aws.amazon.com/cli/latest/reference/s3api/restore-object.html>`__ the storage class of restored objects should remain unchanged. Therefore, for temporary copies, the x-amz-storage-class will continue to reflect the original cloud-tier storage class.


mtime
~~~~
The `mtime` of the transitioned and restored objects should remain unchanged.


Lifecycle
~~~~~~~~
`Temporary` copies are not subjected to any further transition to the cloud. However (as is the case with `cloud-transitioned objects`) they can be deleted via regular LC expiration rules or via external S3 Delete request.
`Permanent` copies are treated as any regular objects and are subjected to any LC rules applicable.


Replication
~~~~~~~~~~
`Temporary` copies are not replicated and will be retained only on the zones the restore request is initiated on.
`Permanent` copies are replicated like other regular objects.


Versioned Objects
~~~~~~~~~~~~~~~~
For versioned objects, if an object has been `cloud-transitioned`, it would be in a non-current state. After a restore, the same non-current object will be updated with the downloaded data, and its HEAD object will be modified accordingly.



Future Work
-----------

* Admin Ops

* Notifications

