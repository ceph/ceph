:orphan:

=========================================================
 rbd-replay -- replay rados block device (RBD) workloads
=========================================================

.. program:: rbd-replay

Synopsis
========

| **rbd-replay** [ *options* ] *replay_file*


Description
===========

**rbd-replay** is a utility for replaying rados block device (RBD) workloads.


Options
=======

.. option:: -c ceph.conf, --conf ceph.conf

   Use ceph.conf configuration file instead of the default /etc/ceph/ceph.conf to
   determine monitor addresses during startup.

.. option:: -p pool, --pool pool

   Interact with the given pool.  Defaults to 'rbd'.

.. option:: --latency-multiplier

   Multiplies inter-request latencies.  Default: 1.

.. option:: --read-only

   Only replay non-destructive requests.

.. option:: --map-image rule

   Add a rule to map image names in the trace to image names in the replay cluster.
   A rule of image1@snap1=image2@snap2 would map snap1 of image1 to snap2 of image2.

.. option:: --dump-perf-counters

   **Experimental**
   Dump performance counters to standard out before an image is closed.
   Performance counters may be dumped multiple times if multiple images are closed,
   or if the same image is opened and closed multiple times.
   Performance counters and their meaning may change between versions.


Examples
========

To replay workload1 as fast as possible::

       rbd-replay --latency-multiplier=0 workload1

To replay workload1 but use test_image instead of prod_image::

       rbd-replay --map-image=prod_image=test_image workload1


Availability
============

**rbd-replay** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`rbd-replay-prep <rbd-replay-prep>`\(8),
:doc:`rbd <rbd>`\(8)
