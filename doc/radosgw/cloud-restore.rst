=============
Cloud Restore
=============

The :doc:`cloud-transition` feature makes it possible to transition objects to a remote
cloud service. The ``cloud-restore`` feature described below enables restoration
of those transitioned objects from the remote S3 endpoints into the local
RGW deployment.

This feature currently enables the restoration of objects transitioned to
S3-compatible cloud services. In order to faciliate this,
the ``retain_head_object`` option should be set to ``true``
in the ``tier-config`` when configuring the storage class.

Objects can be restored using the `S3 RestoreObject <https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html>`_
API. The restored copies will be retained within RGW only for the number
of ``days`` specified. However if ``days`` is not provided, the restored copies
are considered permanent and will be treated as regular objects.
In addition, by enabling the ``allow_read_through`` option,
the `S3 GetObject <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>`_
API can be used to restore the object temporarily.


Cloud Storage Class Tier Configuration
--------------------------------------

The `tier configuration <https://docs.ceph.com/en/latest/radosgw/cloud-transition/#cloud-storage-class-configuration>`_
of the cloud storage class configured for data transition is used to restore
objects as well::

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

The below options have been added to the tier configuration to facilitate object restoration.

* ``restore_storage_class`` (string)

The storage class to which object data is to be restored. Default value is ``STANDARD``.


Read-through Specific Configurables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``allow_read_through`` (``true`` | ``false``)

If true, enables ``read-through``. Objects can then be restored using the ``S3 GetObject`` API.

* ``read_through_restore_days`` (integer)

The duration for which objects restored via ``read-through`` are retained.
Default value is 1 day.

For example:

.. prompt:: bash #

   radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                              --placement-id default-placement \
                                              --storage-class CLOUDTIER \
                                              --tier-config=endpoint=http://XX.XX.XX.XX:YY,\
                                              access_key=<access_key>,secret=<secret>, \
                                              retain_head_object=true, \
                                              restore_storage_class=COLDTIER, \
                                              allow_read_through=true, \
                                              read_through_restore_days=10



S3 Glacier Specific Configurables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To restore objects archived in an S3 Glacier or Tape cloud storage class, the
data must first be restored to the cloud service before being read and
downloaded into RGW. To enable this process, ensure the storage class
is configured with ``--tier-type=cloud-s3-glacier``. Additionally,
the following configurables should be set accordingly:

* ``glacier_restore_days`` (integer)

The duration for which the objects are to be restored on the remote cloud service.

* ``glacier_restore_tier_type`` (``Standard`` | ``Expedited``)

The type of retrieval within the cloud service, which may represent different
pricing. Supported options are ``Standard`` and ``Expedited``.


For example:

.. prompt:: bash #

   radosgw-admin zonegroup placement add --rgw-zonegroup=default \
                                           --placement-id=default-placement \
                                           --storage-class=CLOUDTIER-GLACIER --tier-type=cloud-s3-glacier
   radosgw-admin zonegroup placement modify --rgw-zonegroup default \
                                              --placement-id default-placement \
                                              --storage-class CLOUDTIER \
                                              --tier-config=endpoint=http://XX.XX.XX.XX:YY,\
                                              access_key=XXXXX,secret=YYYYY, \
                                              retain_head_object=true, \
                                              target_storage_class=Glacier, \
                                              ............ \
                                              ............ \
                                              restore_storage_class=COLDTIER, \
                                              glacier_restore_days=2, \
                                              glacier_restore_tier_type=Expedited

::

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


Examples of Restore Objects
---------------------------

Using the S3 RestoreObject CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Th `S3 restore-object <https://docs.aws.amazon.com/cli/latest/reference/s3api/restore-object.html>`_
CLI supports these options:

.. prompt:: bash $

   aws s3api restore-object --bucket <value> \
                              --key <value> \
                              [--version-id <value>] \
                              --restore-request (structure) { \
                                Days=<integer> \
                              }


Note: ``Days`` is optional and if not provided, the object is restored permanently.

Example 1:

.. prompt:: bash $

   aws s3api restore-object --bucket bucket1 --key doc1.rtf \
                              [--version-id 3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo] \
                              --restore-request Days=10 \
                              ....


This will restore the object ``doc1.rtf`` at an optional version,
for the duration of 10 days.

Example 2:

.. prompt:: bash $

   aws s3api restore-object --bucket bucket1 --key doc2.rtf --restore-request {} ....


This will restore the object ``doc2.rtf`` permanently and it will be treated as regular object.


Using the S3 GetObject CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~

Ensure that the ``allow_read_through`` tier-config option is enabled.

Example 3:

.. prompt:: bash $

   aws s3api get-object --bucket bucket1 --key doc3.rtf ....


This will restore the object ``doc3.rtf`` for ``read_through_restore_days`` days.

Note: The above CLI command may time out if object restoration takes too long.
You can verify the restore status before reissuing the command.


Verifying the Restoration Status
--------------------------------
Verify the status of the restoration by issuing
an `S3 HeadObject <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax>`_
request. The response includes the ``x-amz-restore`` header if object restoration
is in progress or a copy of it is already restored.

Example:

.. prompt:: bash $

   aws s3api head-object --key doc1.rtf --bucket bucket1 ....


The ``radosgw-admin`` CLI can be used to check restoration status and other
details.

Example:

.. prompt:: bash $

   radosgw-admin object stat --bucket bucket1 --object doc1.rtf



Restored Object Properties
--------------------------

Storage
~~~~~~~
Objects are restored to the storage class configured via ``restore_storage_class``
in the tier-config. However, as
per `<https://docs.aws.amazon.com/cli/latest/reference/s3api/restore-object.html>`_
the storage class of restored objects should remain unchanged. Therefore, for
temporary copies, the ``x-amz-storage-class`` will continue to reflect the
original cloud-tier storage class.


mtime
~~~~~
The ``mtime`` of the transitioned and restored objects should remain unchanged.


Lifecycle
~~~~~~~~~
``Temporary`` copies are not subject to transition to the cloud. However, as is the
case with cloud-transitioned objects, they can be deleted via regular LC (Life Cycle)
expiration rules or an external S3 ``delete`` request.

``Permanent`` copies are treated as regular objects and are subject to applicable LC
policies.


Replication
~~~~~~~~~~~
``Temporary`` copies are not replicated and will be retained only by the zone
on which the restore request is initiated.

``Permanent`` copies are replicated like other regular objects.


Versioned Objects
~~~~~~~~~~~~~~~~~
For versioned objects, if an object has been cloud-transitioned, it is in a
non-current state. After a restore, the same non-current object will be
updated with the downloaded data, and its ``HEAD`` object will be modified accordingly.



Future Work
-----------

* Admin Ops

* Notifications

