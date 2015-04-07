:orphan:

============================================
 cephfs -- ceph file system options utility
============================================

.. program:: cephfs

Synopsis
========

| **cephfs** [ *path* *command* *options* ]


Description
===========

**cephfs** is a control utility for accessing and manipulating file
layout and location data in the Ceph distributed storage system.

.. TODO format this like a proper man page

Choose one of the following three commands:

- ``show_layout`` View the layout information on a file or directory
- ``set_layout`` Set the layout information on a file or directory
- ``show_location`` View the location information on a file


Options
=======

Your applicable options differ depending on whether you are setting or viewing layout/location.

Viewing options:
----------------

.. option:: -l --offset

    Specify an offset for which to retrieve location data

Setting options:
----------------

.. option:: -u --stripe_unit

   Set the size of each stripe

.. option:: -c --stripe_count

   Set the number of objects to stripe across

.. option:: -s --object_size

   Set the size of the objects to stripe across

.. option:: -p --pool

   Set the pool (by numeric value, not name!) to use

.. option:: -o --osd

   Set the preferred OSD to use as the primary (deprecated and ignored)


Limitations
===========

When setting layout data, the specified object size must evenly divide
by the specified stripe unit. Any parameters you don't set
explicitly are left at the system defaults.

Obviously setting the layout of a file and a directory means different
things. Setting the layout of a file specifies exactly how to place
the individual file. This must be done before writing *any* data to
it. Truncating a file does not allow you to change the layout either.

Setting the layout of a directory sets the "default layout", which is
used to set the file layouts on any files subsequently created in the
directory (or any subdirectory).  Pre-existing files do not have their
layouts changed.

You'll notice that the layout information allows you to specify a
preferred OSD for placement. This feature is unsupported and ignored
in modern versions of the Ceph servers; do not use it.


Availability
============

**cephfs** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer
to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
