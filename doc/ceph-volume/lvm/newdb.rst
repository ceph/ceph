.. _ceph-volume-lvm-newdb:

``new-db``
===========

Attaches the given logical volume to OSD as a DB.
Logical volume name format is vg/lv. Fails if OSD has already got attached DB.

Attach vgname/lvname as a DB volume to OSD 1::

    ceph-volume lvm new-db --osd-id 1 --osd-fsid 55BD4219-16A7-4037-BC20-0F158EFCC83D --target vgname/new_db
