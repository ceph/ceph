.. _ceph-volume-lvm-migrate:

``migrate``
===========

Moves BlueFS data from source volume(s) to the target one, source volumes
(except the main, i.e. data or block one) are removed on success.

LVM volumes are permitted for Target only, both already attached or new one.

In the latter case it is attached to the OSD replacing one of the source
devices.

Following replacement rules apply (in the order of precedence, stop
on the first match):

    - if source list has DB volume - target device replaces it.
    - if source list has WAL volume - target device replaces it.
    - if source list has slow volume only - operation is not permitted,
      requires explicit allocation via new-db/new-wal command.

Moves BlueFS data from main device to LV already attached as DB::

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data --target vgname/db

Moves BlueFS data from shared main device to LV which will be attached as a
new DB::

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data --target vgname/new_db

Moves BlueFS data from DB device to new LV, DB is replaced::

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from db --target vgname/new_db

Moves BlueFS data from main and DB devices to new LV, DB is replaced::

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data db --target vgname/new_db

Moves BlueFS data from main, DB and WAL devices to new LV, WAL is  removed and
DB is replaced::

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data db wal --target vgname/new_db

Moves BlueFS data from main, DB and WAL devices to main device, WAL and DB are
removed::

    ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from db wal --target vgname/data
