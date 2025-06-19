Local Pool Module
=================

.. mgr_module:: localpool

The ``localpool`` module can automatically create RADOS pools that are
localized to a subset of the overall cluster.  For example, by default, it
creates a pool for each distinct ``rack`` in the cluster. This can be
useful for deployments where it is desirable to distribute some data locally
and other data globally across the cluster. One use case is measuring
performance and testing behavior of specific drive, NIC, or chassis models in
isolation.

Enabling
--------

To enable the ``localpool`` module, run the following command:

.. prompt:: bash #

   ceph mgr module enable localpool

Configuring
-----------

The ``localpool`` module understands the following options:

.. confval:: subtree
.. confval:: failure_domain
.. confval:: pg_num
.. confval:: num_rep
.. confval:: min_size
.. confval:: prefix

The default is ``by-$subtreetype-``.

These options are set via the ``config-key`` interface. For example, to change
the replication level to 2x with 64 PGs, run the following two commands:

.. prompt:: bash #

   ceph config set mgr mgr/localpool/num_rep 2
   ceph config set mgr mgr/localpool/pg_num 64

.. mgr_module:: None
