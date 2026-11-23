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

If ``prefix`` is empty (the default), each pool is named
``by-<subtree type>-<CRUSH bucket name>``, for example ``by-rack-rack1``.

These are Manager module options. They are stored in the Ceph configuration
database and are set with ``ceph config set mgr mgr/localpool/<option> <value>``.
The module applies them only when it creates a pool, and it does not change or
remove pools that it has already created. Because the module creates pools as
soon as it is enabled, set the options before you enable it. For example, to
have new pools created with two replicas and 64 PGs, run the following two
commands:

.. prompt:: bash #

   ceph config set mgr mgr/localpool/num_rep 2
   ceph config set mgr mgr/localpool/pg_num 64

.. mgr_module:: None
