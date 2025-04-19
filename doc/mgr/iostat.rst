.. _mgr-iostat-overview:

iostat
======

This module shows the current throughput and IOPS done on the Ceph cluster.

Enabling
--------

To check if the *iostat* module is enabled, run:

.. prompt:: bash #

   ceph mgr module ls

The module can be enabled with:

.. prompt:: bash #

   ceph mgr module enable iostat

To execute the module, run:

.. prompt:: bash #

   ceph iostat

To change the frequency at which the statistics are printed, use the ``-p``
option:

.. prompt:: bash #

   ceph iostat -p <period in seconds>

For example, use the following command to print the statistics every 5 seconds:

.. prompt:: bash #

   ceph iostat -p 5

To stop the module, press Ctrl-C.
