===========================
 Installing a Ceph cluster
===========================

.. todo:: write me

Authentication is optional but very much recommended.

Basically, everything somebody needs to go through to build a new
cluster when not cheating via vstart or teuthology, but without
mentioning all the design tradeoffs and options like journaling
locations or filesystems

At this point, either use 1 or 3 mons, point to :doc:`grow/mon`

OSD installation
================

btrfs
-----

what does btrfs give you (the journaling thing)


ext4/ext3
---------

.. _xattr:

Enabling extended attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

how to enable xattr on ext4/3
