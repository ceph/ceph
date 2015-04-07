:orphan:

======================================================
 osdmaptool -- ceph osd cluster map manipulation tool
======================================================

.. program:: osdmaptool

Synopsis
========

| **osdmaptool** *mapfilename* [--print] [--createsimple *numosd*
  [--pgbits *bitsperosd* ] ] [--clobber]


Description
===========

**osdmaptool** is a utility that lets you create, view, and manipulate
OSD cluster maps from the Ceph distributed storage system. Notably, it
lets you extract the embedded CRUSH map or import a new CRUSH map.


Options
=======

.. option:: --print

   will simply make the tool print a plaintext dump of the map, after
   any modifications are made.

.. option:: --clobber

   will allow osdmaptool to overwrite mapfilename if changes are made.

.. option:: --import-crush mapfile

   will load the CRUSH map from mapfile and embed it in the OSD map.

.. option:: --export-crush mapfile

   will extract the CRUSH map from the OSD map and write it to
   mapfile.

.. option:: --createsimple numosd [--pgbits bitsperosd]

   will create a relatively generic OSD map with the numosd devices.
   If --pgbits is specified, the initial placement group counts will
   be set with bitsperosd bits per OSD. That is, the pg_num map
   attribute will be set to numosd shifted by bitsperosd.


Example
=======

To create a simple map with 16 devices::

        osdmaptool --createsimple 16 osdmap --clobber

To view the result::

        osdmaptool --print osdmap


Availability
============

**osdmaptool** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please
refer to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`crushtool <crushtool>`\(8),
