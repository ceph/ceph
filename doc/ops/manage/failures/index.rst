.. _failures-osd:

==========================
 Recovering from failures
==========================

The current health of the Ceph cluster, as known by the monitors, can
be checked with the ``ceph health`` command.  If all is well, you get::

 $ ceph health
 HEALTH_OK

If there are problems, you will see something like::

 $ ceph health
 HEALTH_WARN short summary of problem(s)

or::

 $ ceph health
 HEALTH_ERROR short summary of very serious problem(s)

To get more detail::

 $ ceph health detail
 HEALTH_WARN short description of problem

 one problem
 another problem
 yet another problem
 ...

.. toctree::

 radosgw

