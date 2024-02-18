:orphan:

==============================================================
ceph-objectstore-tool -- modify or examine the state of an OSD
==============================================================

Synopsis
========


| **ceph-objectstore-tool** --data-path *path to osd* [--op *list* ]



Possible object operations:

* (get|set)-bytes [file]
* set-(attr|omap) [file]
* (get|rm)-attr|omap)
* get-omaphdr
* set-omaphdr [file]
* list-attrs
* list-omap
* remove|removeall
* dump
* set-size
* clear-data-digest
* remove-clone-metadata 


Description
===========

**ceph-objectstore-tool** is a tool for modifying the state of an OSD. It facilitates manipulating an object's content, removing an object, listing the omap, manipulating the omap header, manipulating the omap key, listing object attributes, and manipulating object attribute keys.

**ceph-objectstore-tool** provides two main modes: (1) a mode that specifies the "--op" argument (for example, **ceph-objectstore-tool** --data-path $PATH_TO_OSD --op $SELECT_OPERATION [--pgid $PGID] [--dry-run]), and (2) a mode for positional object operations. If the second mode is used, the object can be specified by ID or by the JSON output of the --op list. 

| **ceph-objectstore-tool** --data-path *path to osd* [--pgid *$PG_ID* ][--op *command*]
| **ceph-objectstore-tool** --data-path *path to osd* [ --op *list $OBJECT_ID*]

Possible -op commands::

* info
* log
* remove
* mkfs
* fsck
* repair
* fuse
* dup
* export
* export-remove
* import
* list
* list-slow-omap
* fix-lost
* list-pgs
* dump-super
* meta-list
* get-osdmap
* set-osdmap
* get-inc-osdmap
* set-inc-osdmap
* mark-complete
* reset-last-complete
* update-mon-db
* dump-export
* trim-pg-log

Installation
============

The `ceph-osd` package provides **ceph-objectstore-tool**.


Examples
========

Modifying Objects
-----------------
These commands modify state of an OSD. The OSD must not be running when ceph-objectstore-tool is used.

Listing Objects and Placement Groups
------------------------------------

Make sure that the target OSD is down::

   systemctl status ceph-osd@$OSD_NUMBER

Identify all objects within an OSD::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --op list

Identify all objects within a placement group::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op list

Identify the placement group (PG) that an object belongs to::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --op list $OBJECT_ID


Fixing Lost Objects   
-------------------

Make sure the OSD is down::

   systemctl status ceph-osd@OSD_NUMBER

Fix all lost objects::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --op fix-lost

Fix all the lost objects within a specified placement group::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID --op fix-lost

Fix a lost object by its identifier::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --op fix-lost $OBJECT_ID

Fix legacy lost objects::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --op fix-lost


Manipulating an object's content
--------------------------------

1. Make sure that the target OSD is down::
   
    systemctl status ceph-osd@$OSD_NUMBER

2. Find the object by listing the objects of the OSD or placement group.

3. Before setting the bytes on the object, make a backup and a working copy of the object. Here is the syntactic form of that command::
   
    ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-bytes > $OBJECT_FILE_NAME

For example::

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' get-bytes > zone_info.default.backup

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' get-bytes > zone_info.default.working-copy

The first command creates the back-up copy, and the second command creates the working copy.

4. Edit the working copy object file.

5. Set the bytes of the object::
     
     ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-bytes < $OBJECT_FILE_NAME

For example::

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' set-bytes < zone_info.default.working-copy
 

Removing an Object
------------------

Use **ceph-objectstore-tool** to remove objects. When an object is removed, its contents and references are removed from the placement group (PG).

Remove an object (syntax)::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT remove

Remove an object (example)::

[root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' remove


Listing the Object Map
----------------------

Use the ceph-objectstore-tool to list the contents of the object map (OMAP). The output is a list of keys.


1. Verify the appropriate OSD is down:

   Syntax::

    systemctl status ceph-osd@$OSD_NUMBER

   Example::

    [root@osd ~]# systemctl status ceph-osd@1

2. List the object map:

   Syntax::

    ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-omap

   Example::

    [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' list-omap


Manipulating the Object Map Header
----------------------------------
The **ceph-objectstore-tool** utility will output the object map (OMAP) header with the values associated with the object's keys.

Prerequisites
^^^^^^^^^^^^^

    * Having root access to the Ceph OSD node.
    * Stopping the ceph-osd daemon. 

Procedure
^^^^^^^^^

  Verify that the target OSD is down:

  Syntax::

    systemctl status ceph-osd@$OSD_NUMBER

  Example::

    [root@osd ~]# systemctl status ceph-osd@1

  Get the object map header:

  Syntax::

        ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omaphdr > $OBJECT_MAP_FILE_NAME

  Example::

        [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}'  get-omaphdr > zone_info.default.omaphdr.txt

  Set the object map header:

  Syntax::

        ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omaphdr < $OBJECT_MAP_FILE_NAME

  Example::

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}'  set-omaphdr < zone_info.default.omaphdr.txt


Manipulating the Object Map Key
-------------------------------

Use the **ceph-objectstore-tool** utility to change the object map (OMAP) key.
Provide the data path, the placement group identifier (PG ID), the object, and
the key in the OMAP.

Prerequisites
^^^^^^^^^^^^^

    * Having root access to the Ceph OSD node.
    * Stopping the ceph-osd daemon. 

Commands
^^^^^^^^

Run the commands in this section as ``root`` on an OSD node.

* **Getting the object map key**

   Syntax:

   .. code-block:: ini 
     
      ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-omap $KEY > $OBJECT_MAP_FILE_NAME

   Example::

    ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}'  get-omap "" > zone_info.default.omap.txt

* **Setting the object map key**

   Syntax:

   .. code-block:: ini 

      ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT set-omap $KEY < $OBJECT_MAP_FILE_NAME

   Example::

    ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' set-omap "" < zone_info.default.omap.txt

* **Removing the object map key**

   Syntax:

   .. code-block:: ini 

      ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-omap $KEY

   Example::

    ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' rm-omap ""


Listing an Object's Attributes
-------------------------------

Use the **ceph-objectstore-tool** utility to list an object's attributes. The output provides you with the object's keys and values.
Note

Prerequisites
^^^^^^^^^^^^^

    * Having root access to the Ceph OSD node.
    * Stopping the ceph-osd daemon. 

Procedure
^^^^^^^^^

   Verify that the target OSD is down:

   Syntax::

    systemctl status ceph-osd@$OSD_NUMBER

   Example::

    [root@osd ~]# systemctl status ceph-osd@1

   List the object's attributes:

   Syntax::

    ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT list-attrs

   Example::

    [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' list-attrs


MANIPULATING THE OBJECT ATTRIBUTE KEY
-------------------------------------

Use the ceph-objectstore-tool utility to change an object's attributes. To manipulate the object's attributes you need the data and journal paths, the placement group identifier (PG ID), the object, and the key in the object's attribute.
Note

Prerequisites

    * Having root access to the Ceph OSD node.  
    * Stopping the ceph-osd daemon. 

Procedure

    Verify that the target OSD is down.

 Syntax::

    systemctl status ceph-osd@$OSD_NUMBER

 Example::

    [root@osd ~]# systemctl status ceph-osd@1

 Get the object's attributes:

 Syntax::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT get-attrs $KEY > $OBJECT_ATTRS_FILE_NAME

 Example::

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0  --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' get-attrs "oid" > zone_info.default.attr.txt

 Set an object's attributes:

 Syntax::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT  set-attrs $KEY < $OBJECT_ATTRS_FILE_NAME

 Example::

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' set-attrs "oid" < zone_info.default.attr.txt

 Remove an object's attributes:

 Syntax::

   ceph-objectstore-tool --data-path $PATH_TO_OSD --pgid $PG_ID $OBJECT rm-attrs $KEY

 Example::

   [root@osd ~]# ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --pgid 0.1c '{"oid":"zone_info.default","key":"","snapid":-2,"hash":235010478,"max":0,"pool":11,"namespace":""}' rm-attrs "oid"


Options
=======

.. option:: --help          

   produce help message

.. option:: --type arg        

   Arg is one of [bluestore (default), memstore]. This option is needed only if the tool can't tell the type from --data-path.
 
.. option:: --data-path arg

   path to object store, mandatory
   
.. option:: --journal-path arg

   path to journal, use if tool can't find it
   
.. option:: --pgid arg

   PG id, mandatory for info, log, remove, export, export-remove, mark-complete, trim-pg-log
                             
.. option:: --pool arg

   Pool name

.. option:: --op arg

   Arg is one of [info, log, remove, mkfs, fsck, repair, fuse, dup, export, export-remove, import, list, fix-lost, list-pgs, dump-super, meta-list, get-osdmap, set-osdmap, get-inc-osdmap, set-inc-osdmap, mark-complete, reset-last-complete, update-mon-db, dump-export, trim-pg-log]

.. option:: --epoch arg

   epoch# for get-osdmap and get-inc-osdmap, the current epoch in use if not specified

.. option:: --file arg             
   
   path of file to export, export-remove, import, get-osdmap, set-osdmap, get-inc-osdmap or set-inc-osdmap

.. option:: --mon-store-path arg

   path of monstore to update-mon-db

.. option:: --fsid arg

   fsid for new store created by mkfs

.. option:: --target-data-path arg

   path of target object store (for --op dup)
   
.. option:: --mountpoint arg

   fuse mountpoint

.. option:: --format arg (=json-pretty) 

   Output format which may be json, json-pretty, xml, xml-pretty

.. option:: --debug

   Enable diagnostic output to stderr

.. option:: --force

   Ignore some types of errors and proceed with operation - USE WITH CAUTION: CORRUPTION POSSIBLE NOW OR IN THE FUTURE

.. option:: --skip-journal-replay

   Disable journal replay

.. option:: --skip-mount-omap

   Disable mounting of omap

.. option:: --head

   Find head/snapdir when searching for objects by name

.. option:: --dry-run

   Don't modify the objectstore

.. option:: --namespace arg

   Specify namespace when searching for objects

.. option:: --rmtype arg      

   Specify corrupting object removal 'snapmap' or 'nosnapmap' - TESTING USE ONLY



Error Codes
===========
"Mount failed with '(11) Resource temporarily unavailable" - This might mean that you have attempted to run **ceph-objectstore-tool** on a running OSD.

Availability
============

**ceph-objectstore-tool** is part of Ceph, a massively scalable, open-source, distributed storage system. **ceph-objectstore-tool** is provided by the package `ceph-osd`. Refer to the Ceph documentation at http://ceph.com/docs for more information.
