.. _ceph-volume-overview:

Overview
--------
The ``ceph-volume`` tool aims to be a single purpose command line tool to deploy
logical volumes as OSDs, trying to maintain a similar API to ``ceph-disk`` when
preparing, activating, and creating OSDs.

It deviates from ``ceph-disk`` by not interacting or relying on the udev rules
that come installed for Ceph. These rules allow automatic detection of
previously setup devices that are in turn fed into ``ceph-disk`` to activate
them.


``ceph-volume lvm``
-------------------
By making use of :term:`LVM tags`, the :ref:`ceph-volume-lvm` sub-command is
able to store and later re-discover and query devices associated with OSDs so
that they can later activated. This includes support for lvm-based technologies
like dm-cache as well.

For ``ceph-volume``, the use of dm-cache is transparent, there is no difference
for the tool, and it treats dm-cache like a plain logical volume.
