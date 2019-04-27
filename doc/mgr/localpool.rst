Local Pool Module
=================

The *localpool* module can automatically create RADOS pools that are
localized to a subset of the overall cluster.  For example, by default, it will
create a pool for each distinct rack in the cluster.  This can be useful for some
deployments that want to distribute some data locally as well as globally across the cluster .

Enabling
--------

The *localpool* module is enabled with::

  ceph mgr module enable localpool

Configuring
-----------

The *localpool* module understands the following options:

* **subtree** (default: `rack`): which CRUSH subtree type the module
  should create a pool for.
* **failure_domain** (default: `host`): what failure domain we should
  separate data replicas across.
* **pg_num** (default: `128`): number of PGs to create for each pool
* **num_rep** (default: `3`): number of replicas for each pool.
  (Currently, pools are always replicated.)
* **min_size** (default: none): value to set min_size to (unchanged from Ceph's default if this option is not set)
* **prefix** (default: `by-$subtreetype-`): prefix for the pool name.

These options are set via the config-key interface.  For example, to
change the replication level to 2x with only 64 PGs, ::

  ceph config set mgr mgr/localpool/num_rep 2
  ceph config set mgr mgr/localpool/pg_num 64
