RBD Incremental Backup
======================

This is a simple streaming file format for representing a diff between
two snapshots (or a snapshot and the head) of an RBD image.

Header
------

le32: string length, always 13
"rbd diff v1"

Record
------

Every record has a one byte "tag" that identifies the record type, followed by some other
data.

From snap
---------

u8: 'f'
le32: snap name length
snap name

To snap
-------

u8: 't'
le32: snap name length
snap name

Size
----

u8: 's'
u64: (ending) image size

Updated data
------------

u8: 'w'
le64: offset
le64: length
length bytes of actual data

Zero data
---------

u8: 'z'
le64: offset
le64: length





