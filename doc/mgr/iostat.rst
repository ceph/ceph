.. _mgr-iostat-overview:

iostat
======

The ``iostat`` module reports metrics for cluster throughput and IOPS.

Enabling
--------

To determine whether the ``iostat`` module is enabled, run the following
command:

.. prompt:: bash #

   ceph mgr module ls

To enable the ``iostat`` module, run the following command:

.. prompt:: bash #

   ceph mgr module enable iostat

To print the statistics, run the following command. It prints a new line every
second until you stop it:

.. prompt:: bash #

   ceph iostat

To change the frequency at which the statistics are printed, use the ``-p``
option:

.. prompt:: bash #

   ceph iostat -p <period in seconds>

For example, use the following command to print the statistics every 5
seconds:

.. prompt:: bash #

   ceph iostat -p 5

To stop the output, press ``Ctrl-C``. This ends only the ``ceph`` command; the
``iostat`` module keeps running in the Manager.
