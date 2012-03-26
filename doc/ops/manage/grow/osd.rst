============================
 Resizing the RADOS cluster
============================

Adding a new OSD to the cluster
===============================

Briefly...

#. Allocate a new OSD id::

   $ ceph osd create
   123

#. Make sure ceph.conf is valid for the new OSD.

#. Initialize osd data directory::

   $ ceph-osd -i 123 --mkfs --mkkey

#. Register the OSD authentication key::

   $ ceph auth add osd.123 osd 'allow *' mon 'allow rwx' -i /var/lib/ceph/osd-data/123/keyring

#. Adjust the CRUSH map to allocate data to the new device (see :ref:`adjusting-crush`).


Removing OSDs
=============

Briefly...

#. Stop the daemon

#. Remove it from the CRUSH map::

   $ ceph osd crush remove osd.123

#. Remove it from the osd map::

   $ ceph osd rm 123

See also :ref:`failures-osd`.
