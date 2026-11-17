==================
Backup Sync Module
==================

The Backup Sync module maintains a backup zone: a versioned copy of the
buckets in other zones that follows both their updates *and* their
deletions, while retaining the data that was overwritten or removed.

Like the :doc:`archive zone <archive-sync-module>`, a backup zone keeps
bucket versioning permanently enabled and stores every synced write as a
new object version. The difference is in how deletions are handled:

* An archive zone ignores deletions in other zones. The latest copy of
  an object always remains visible.
* A backup zone replicates deletions as *delete markers*. The current
  view of a backup bucket matches the source bucket, and the prior
  versions remain available as noncurrent versions until lifecycle
  policy purges them.

This makes it possible to restore an overwritten or deleted object from
the backup zone, while keeping the underlying raw storage capacity that
the retained history consumes under control with lifecycle rules that apply
only to the backup zone (see `Lifecycle Policies on Backup Zones`_).


Synchronization Behavior
------------------------

The following table shows how operations in a source zone are reflected
in the backup zone:

.. list-table::
   :header-rows: 1

   * - Source operation
     - Backup current version
     - Retained versions
   * - ``PUT`` (any bucket versioning state)
     - the new object
     - all previous versions
   * - ``DELETE`` of an unversioned object
     - delete marker
     - all previous versions
   * - ``DELETE`` without ``versionId`` on a versioned bucket
     - delete marker
     - all previous versions
   * - ``DELETE ?versionId=`` of the latest version
     - delete marker
     - all previous versions, including the deleted one
   * - ``DELETE ?versionId=`` of an older version
     - unchanged
     - all previous versions, plus a noncurrent delete marker
   * - Re-upload after a delete
     - the new object
     - all previous versions and delete markers

For versioned source buckets the backup zone reuses the source's version
IDs, so a version can be fetched from the backup zone with the same
``versionId`` it had in the source zone. For unversioned source buckets
the backup zone generates its own version IDs.

Removing a bucket in a source zone does not remove it from the backup
zone. Instead, the backup bucket is renamed to
``<bucket-name>-deleted-<suffix>`` and its contents are kept.

Objects in the backup zone can be removed only through gateways
associated with the backup zone, or by lifecycle processing in the
backup zone. To prevent removal through gateways as well, configure the
zone as read-only (see `Read-Only Backup Zones`_).


Lifecycle Policies on Backup Zones
----------------------------------

Lifecycle configuration is part of bucket metadata and is therefore
replicated to every zone, including the backup zone. To keep a rule from
affecting the source zones, and a source rule from affecting the backup,
the ``BackupZone`` filter element (an RGW extension, analogous to
``ArchiveZone``) controls where a rule is evaluated:

* Rules **with** ``<BackupZone/>`` in their ``Filter`` are evaluated
  only on backup zones.
* On a backup zone, rules **without** ``<BackupZone/>`` are ignored.

``BackupZone`` can be combined with the other filter elements
(``Prefix``, ``Tag``, ``ObjectSizeGreaterThan``,
``ObjectSizeLessThan``) inside ``And``.

A backup bucket must keep every version that may still be needed to
restore a source object, so ``BackupZone`` rules follow
``NoncurrentVersionExpiration`` semantics that differ from standard S3:

``NewerNoncurrentVersions``
    Caps the number of noncurrent versions kept per object. Once an
    object has more noncurrent versions than this, the oldest ones are
    purged **regardless of their age**. The cap applies to live and
    deleted objects alike. Delete markers do not count toward the cap.
    When omitted, the number of versions is not limited.

``NoncurrentDays``
    Applies only to *removed* objects, that is, objects whose current
    version in the backup zone is a delete marker. All noncurrent
    versions of such an object are purged once the delete marker is
    ``NoncurrentDays`` old. The retention period is counted from the
    deletion, not from when each version became noncurrent, which
    guarantees a full restore window after an object is deleted. Live
    objects are never purged by age.

``ExpiredObjectDeleteMarker``
    Removes the delete marker of a removed object once all its versions
    have been purged.

``Expiration`` with ``Days`` or ``Date``
    Not allowed in a ``BackupZone`` rule. Expiring current versions
    would hide objects that still exist in the source zone, so such a
    configuration is rejected with ``InvalidRequest``.

.. note:: If ``NewerNoncurrentVersions`` is set, a version that still
   exists in a versioned source bucket may be purged from the backup
   zone once it falls beyond the cap. Choose the cap with the source
   buckets' own retention in mind, or omit it to never purge versions
   of live objects.

The following rule keeps at most ten old versions of every object and
purges removed objects 90 days after their deletion:

.. code-block:: xml

   <LifecycleConfiguration>
     <Rule>
       <ID>backup-retention</ID>
       <Filter>
         <BackupZone/>
       </Filter>
       <Status>Enabled</Status>
       <Expiration>
         <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
       </Expiration>
       <NoncurrentVersionExpiration>
         <NewerNoncurrentVersions>10</NewerNoncurrentVersions>
         <NoncurrentDays>90</NoncurrentDays>
       </NoncurrentVersionExpiration>
     </Rule>
   </LifecycleConfiguration>

Object lock retention and legal holds are honored: locked versions are
not purged.


Backup Sync Tier Type Configuration
-----------------------------------

How to Configure
~~~~~~~~~~~~~~~~

See :ref:`Multisite Configuration <multisite>` for multisite configuration
instructions. The backup sync module requires the creation of a new zone. The
zone tier type needs to be defined as ``backup``:

.. prompt:: bash #

   radosgw-admin zone create --rgw-zonegroup={zone-group-name} \
                               --rgw-zone={zone-name} \
                               --endpoints={http://fqdn}[,{http://fqdn}] \
                               --tier-type=backup

Then commit the period:

.. prompt:: bash #

   radosgw-admin period update --commit

Read-Only Backup Zones
~~~~~~~~~~~~~~~~~~~~~~

A backup zone can be marked read-only to protect the retained data from
S3 clients:

.. prompt:: bash #

   radosgw-admin zone modify --rgw-zone={zone-name} --read-only
   radosgw-admin period update --commit

On a read-only backup zone:

* Modifying S3 requests from regular users, such as ``PUT``,
  ``DELETE``, ``DELETE ?versionId=``, multipart uploads and
  ``PUT ?lifecycle``, are rejected with ``AccessDenied``.
* Read requests, including version listings and ``GET ?versionId=``,
  are served as usual, so objects can still be restored from the backup
  zone.
* Data and metadata sync from the other zones continue, because they
  are not subject to the read-only check.
* Lifecycle processing continues, so ``BackupZone`` rules still purge
  versions.

Keep the following in mind:

* Set lifecycle configuration in a source zone. A ``PUT ?lifecycle``
  sent to a read-only backup zone is rejected before it can be forwarded
  to the metadata master. The configuration replicates to the backup
  zone with the bucket metadata.
* To restore an object, read the version from a backup zone endpoint
  and write it to a source zone endpoint. A single copy request cannot
  span zones.
* The read-only flag applies to S3 clients only. ``radosgw-admin`` and
  system users can still modify data in the backup zone.
* A backup zone never exports data to other zones, whether or not it is
  read-only. The flag protects the backup zone's own contents; it is not
  needed to keep writes from reaching the source zones.
* Do not make a backup zone the master zone of its zonegroup.
