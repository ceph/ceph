.. _mgr-nvmeof:

==============
NVMe-oF Module
==============

The ``nvmeof`` Manager module creates the ``.nvmeof`` pool, in which
:ref:`NVMe-oF gateways <ceph-nvmeof>` store the state of their gateway groups
as OMAP objects. The module was added in Tentacle 20.2.3.

Enabling the Module
===================

The module is enabled by default on a cluster that was first deployed with
Tentacle 20.2.3 or later, because the ``mgr_initial_modules`` option lists it.
That option is read only when a cluster is first started, so on a cluster that
was upgraded from an earlier release, enable the module with the following
command before you deploy NVMe-oF gateways:

.. prompt:: bash #

   ceph mgr module enable nvmeof

To see whether the module is enabled, run the following command:

.. prompt:: bash #

   ceph mgr module ls

The ``.nvmeof`` Pool
====================

When ``ceph orch apply nvmeof`` deploys a gateway group without naming a pool,
or names ``.nvmeof``, the module creates the ``.nvmeof`` pool if it does not
exist. For example:

.. prompt:: bash #

   ceph orch apply nvmeof --group group1 --placement="host01 host02"

The same is true of an ``nvmeof`` service specification that is applied with
``ceph orch apply -i`` and that names no pool or names ``.nvmeof``.

The module creates a replicated pool with one placement group, which the
PG autoscaler will grow to at most 32 placement groups, and enables the
``nvmeof-meta`` application on the pool.

If the module is not enabled, the command fails with the following error:

.. code-block:: none

   NVMe-oF support requires the nvmeof manager module to be enabled before proceeding. Enable it with: ceph mgr module enable nvmeof

Using Another Pool
==================

To store the state of a gateway group in a pool with a custom name, specify the pool
with ``--pool`` when running a command of the following form:

.. prompt:: bash #

   ceph orch apply nvmeof --pool mypool --group group1 --placement="host01 host02"

The custom-named pool must be created before you deploy the gateways. The pool
must support OMAP: use a replicated pool. Vampire and later releases may allow an erasure-coded
pool on which ``allow_ec_optimizations`` is enabled, but a replicated pool is still
advised.  Enabling ``allow_ec_optimizations`` requires all OSDs to run Umbrella or later,
and erasure-coded pools on Crimson OSDs do not support OMAP.
