:orphan:

====================================================================================
 rbd-replay-prep -- prepare captured rados block device (RBD) workloads for replay
====================================================================================

.. program:: rbd-replay-prep

Synopsis
========

| **rbd-replay-prep** [ --window *seconds* ] [ --anonymize ] *trace_dir* *replay_file*


Description
===========

**rbd-replay-prep** processes raw rados block device (RBD) traces to prepare them for **rbd-replay**.


Options
=======

.. option:: --window seconds

   Requests further apart than 'seconds' seconds are assumed to be independent.

.. option:: --anonymize

   Anonymizes image and snap names.

.. option:: --verbose

   Print all processed events to console

Examples
========

To prepare workload1-trace for replay::

       rbd-replay-prep workload1-trace/ust/uid/1000/64-bit workload1


Availability
============

**rbd-replay-prep** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`rbd-replay <rbd-replay>`\(8),
:doc:`rbd <rbd>`\(8)
