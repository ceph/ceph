.. _ceph-volume-lvm-newwal:

``new-wal``
===========

Attaches the given logical volume to the given OSD as a WAL volume.
Logical volume format is vg/lv. Fails if OSD has already got attached DB.

Attach vgname/lvname as a WAL volume to OSD 1::

    ceph-volume lvm new-wal --osd-id 1 --osd-fsid 55BD4219-16A7-4037-BC20-0F158EFCC83D --target vgname/new_wal
