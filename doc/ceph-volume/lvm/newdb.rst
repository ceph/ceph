.. _ceph-volume-lvm-newdb:

``new-db``
===========

Attaches the given logical volume to OSD as a DB.
Logical volume name format is vg/lv. Fails if OSD has already got attached DB.

Attach vgname/lvname as a DB volume to OSD 1::

    ceph-volume lvm new-db --osd-id 1 --osd-fsid 55BD4219-16A7-4037-BC20-0F158EFCC83D --target vgname/new_db

Reversing BlueFS Spillover to Slow Devices
------------------------------------------

Under certain circumstances, OSD RocksDB databases spill onto slow storage and
the Ceph cluster returns specifics regarding BlueFS spillover warnings. ``ceph
health detail`` returns these spillover warnings.  Here is an example of a
spillover warning::

   osd.76 spilled over 128 KiB metadata from 'db' device (56 GiB used of 60 GiB) to slow device

To move this DB metadata from the slower device to the faster device, take the
following steps:

#. Expand the database's logical volume (LV):

   .. prompt:: bash #

      lvextend -l ${size} ${lv}/${db} ${ssd_dev}

#. Stop the OSD:

   .. prompt:: bash #

      cephadm unit --fsid $cid --name osd.${osd} stop

#. Run the ``bluefs-bdev-expand`` command:

   .. prompt:: bash #

      cephadm shell --fsid $cid --name osd.${osd} -- ceph-bluestore-tool bluefs-bdev-expand --path /var/lib/ceph/osd/ceph-${osd}

#. Run the ``bluefs-bdev-migrate`` command:

   .. prompt:: bash #

      cephadm shell --fsid $cid --name osd.${osd} -- ceph-bluestore-tool bluefs-bdev-migrate --path /var/lib/ceph/osd/ceph-${osd} --devs-source /var/lib/ceph/osd/ceph-${osd}/block --dev-target /var/lib/ceph/osd/ceph-${osd}/block.db 

#. Restart the OSD:

   .. prompt:: bash #

      cephadm unit --fsid $cid --name osd.${osd} start

.. note:: *The above procedure was developed by Chris Dunlop on the [ceph-users] mailing list, and can be seen in its original context here:* `[ceph-users] Re: Fixing BlueFS spillover (pacific 16.2.14) <https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/message/POPUFSZGXR3P2RPYPJ4WJ4HGHZ3QESF6/>`_
