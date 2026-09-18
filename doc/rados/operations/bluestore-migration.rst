.. _rados_operations_bluestore_migration:

=====================
 BlueStore Migration
=====================

BlueStore is the only OSD back end in this release. Filestore was deprecated in
the Reef release and has since been removed: a Filestore OSD cannot start on
this release, so there is nothing left to migrate on a cluster that runs it.

If your cluster still contains Filestore OSDs, it is running Quincy or an
earlier release. Migrate those OSDs to BlueStore *before* you upgrade, by
following the migration guide in the documentation for the release that you are
running. For example, for Quincy see
https://docs.ceph.com/en/quincy/rados/operations/bluestore-migration/.

To list any Filestore OSDs before upgrading, run the following command:

.. prompt:: bash #

   ceph report | jq -c '."osd_metadata" | .[] | select(.osd_objectstore | contains("filestore")) | {id, osd_objectstore}'

The upgrade must not proceed until this command returns no OSDs.
