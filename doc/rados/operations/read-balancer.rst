.. _read_balancer:

=======================================
Operating the Read (Primary) Balancer
=======================================

You might be wondering: How can I improve performance in my Ceph cluster?
One important data point you can check is the ``read_balance_score`` on each
of your replicated pools.

This metric, available via ``ceph osd pool ls detail`` (see :ref:`rados_pools`
for more details) indicates read performance, or how balanced the primaries are
for each replicated pool. In most cases, if a ``read_balance_score`` is above 1
(for instance, 1.5), this means that your pool has unbalanced primaries and that
you may want to try improving your read performance with the read balancer.

Online Optimization
===================

At present, there is no online option for the read balancer. However, we plan to add
the read balancer as an option to the :ref:`balancer` in the next Ceph version
so it can be enabled to run automatically in the background like the upmap balancer.

Offline Optimization
====================

Primaries are updated with an offline optimizer that is built into the
:ref:`osdmaptool`.

#. Grab the latest copy of your osdmap:

   .. prompt:: bash $

      ceph osd getmap -o om

#. Run the optimizer:

   .. prompt:: bash $

      osdmaptool om --read out.txt --read-pool <pool name> [--vstart] 

   It is highly recommended that you run the capacity balancer before running the
   balancer to ensure optimal results. See :ref:`upmap` for details on how to balance
   capacity in a cluster.

#. Apply the changes:

   .. prompt:: bash $

      source out.txt

   In the above example, the proposed changes are written to the output file
   ``out.txt``. The commands in this procedure are normal Ceph CLI commands
   that can be run in order to apply the changes to the cluster.

   If you are working in a vstart cluster, you may pass the ``--vstart`` parameter
   as shown above so the CLI commands are formatted with the `./bin/` prefix.

   Note that any time the number of pgs changes (for instance, if the pg autoscaler [:ref:`pg-autoscaler`]
   kicks in), you should consider rechecking the scores and rerunning the balancer if needed.

To see some details about what the tool is doing, you can pass
``--debug-osd 10`` to ``osdmaptool``. To see even more details, pass
``--debug-osd 20`` to ``osdmaptool``.

Troubleshooting
===============

Removing pg-upmap-primary mappings
------------------------------------

For scenarios where you need to manually remove ``pg-upmap-primary`` mappings, Ceph provides the following
developer-level commands. These commands should be used with caution, as they directly modify
primary PG mappings and can impact read performance (this excludes any data movement).

.. note::

  Users affected by `#66867 <https://tracker.ceph.com/issues/66867>`_ or `#61948 <https://tracker.ceph.com/issues/61948>`_
  may find these commands useful when dealing with unexpected ``pg-upmap-primary`` behavior.

To remove a specific ``pg-upmap-primary`` mapping, use:

.. prompt:: bash $

   ceph osd rm-pg-upmap-primary <pgid>

If you need to clear **all** ``pg-upmap-primary`` mappings in your cluster, you may use:

.. prompt:: bash $

   ceph osd rm-pg-upmap-primary-all
