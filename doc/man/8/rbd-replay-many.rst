:orphan:

==================================================================================
 rbd-replay-many -- replay a rados block device (RBD) workload on several clients
==================================================================================

.. program:: rbd-replay-many

Synopsis
========

| **rbd-replay-many** [ *options* ] --original-image *name* *host1* [ *host2* [ ... ] ] -- *rbd_replay_args*


Description
===========

**rbd-replay-many** is a utility for replaying a rados block device (RBD) workload on several clients.
Although all clients use the same workload, they replay against separate images.
This matches normal use of librbd, where each original client is a VM with its own image.

Configuration and replay files are not automatically copied to clients.
Replay images must already exist.


Options
=======

.. option:: --original-image name

   Specifies the name (and snap) of the originally traced image.
   Necessary for correct name mapping.

.. option:: --image-prefix prefix

   Prefix of image names to replay against.
   Specifying --image-prefix=foo results in clients replaying against foo-0, foo-1, etc.
   Defaults to the original image name.

.. option:: --exec program

   Path to the rbd-replay executable.

.. option:: --delay seconds

   Delay between starting each client.  Defaults to 0.


Examples
========

Typical usage::

       rbd-replay-many host-0 host-1 --original-image=image -- -c ceph.conf replay.bin

This results in the following commands being executed::

       ssh host-0 'rbd-replay' --map-image 'image=image-0' -c ceph.conf replay.bin
       ssh host-1 'rbd-replay' --map-image 'image=image-1' -c ceph.conf replay.bin


Availability
============

**rbd-replay-many** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`rbd-replay <rbd-replay>`\(8),
:doc:`rbd <rbd>`\(8)
