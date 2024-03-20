.. _cephfs-multimds:

Configuring multiple active MDS daemons
---------------------------------------

*Also known as: multi-mds, active-active MDS*

Each CephFS file system is configured for a single active MDS daemon
by default.  To scale metadata performance for large scale systems, you
may enable multiple active MDS daemons, which will share the metadata
workload with one another.

When should I use multiple active MDS daemons?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You should configure multiple active MDS daemons when your metadata performance
is bottlenecked on the single MDS that runs by default.

Adding more daemons may not increase performance on all workloads.  Typically,
a single application running on a single client will not benefit from an
increased number of MDS daemons unless the application is doing a lot of
metadata operations in parallel.

Workloads that typically benefit from a larger number of active MDS daemons
are those with many clients, perhaps working on many separate directories.


Increasing the MDS active cluster size
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each CephFS file system has a *max_mds* setting, which controls how many ranks
will be created.  The actual number of ranks in the file system will only be
increased if a spare daemon is available to take on the new rank. For example,
if there is only one MDS daemon running, and max_mds is set to two, no second
rank will be created. (Note that such a configuration is not Highly Available
(HA) because no standby is available to take over for a failed rank. The
cluster will complain via health warnings when configured this way.)

Set ``max_mds`` to the desired number of ranks.  In the following examples
the "fsmap" line of "ceph status" is shown to illustrate the expected
result of commands.

::

    # fsmap e5: 1/1/1 up {0=a=up:active}, 2 up:standby

    ceph fs set <fs_name> max_mds 2

    # fsmap e8: 2/2/2 up {0=a=up:active,1=c=up:creating}, 1 up:standby
    # fsmap e9: 2/2/2 up {0=a=up:active,1=c=up:active}, 1 up:standby

The newly created rank (1) will pass through the 'creating' state
and then enter this 'active state'.

Standby daemons
~~~~~~~~~~~~~~~

Even with multiple active MDS daemons, a highly available system **still
requires standby daemons** to take over if any of the servers running
an active daemon fail.

Consequently, the practical maximum of ``max_mds`` for highly available systems
is at most one less than the total number of MDS servers in your system.

To remain available in the event of multiple server failures, increase the
number of standby daemons in the system to match the number of server failures
you wish to withstand.

Decreasing the number of ranks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reducing the number of ranks is as simple as reducing ``max_mds``:

::
    
    # fsmap e9: 2/2/2 up {0=a=up:active,1=c=up:active}, 1 up:standby
    ceph fs set <fs_name> max_mds 1
    # fsmap e10: 2/2/1 up {0=a=up:active,1=c=up:stopping}, 1 up:standby
    # fsmap e10: 2/2/1 up {0=a=up:active,1=c=up:stopping}, 1 up:standby
    ...
    # fsmap e10: 1/1/1 up {0=a=up:active}, 2 up:standby

The cluster will automatically stop extra ranks incrementally until ``max_mds``
is reached.

See :doc:`/cephfs/administration` for more details which forms ``<role>`` can
take.

Note: stopped ranks will first enter the stopping state for a period of
time while it hands off its share of the metadata to the remaining active
daemons.  This phase can take from seconds to minutes.  If the MDS appears to
be stuck in the stopping state then that should be investigated as a possible
bug.

If an MDS daemon crashes or is killed while in the ``up:stopping`` state, a
standby will take over and the cluster monitors will against try to stop
the daemon.

When a daemon finishes stopping, it will respawn itself and go back to being a
standby.


.. _cephfs-pinning:

Manually pinning directory trees to a particular rank
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In multiple active metadata server configurations, a balancer runs which works
to spread metadata load evenly across the cluster. This usually works well
enough for most users but sometimes it is desirable to override the dynamic
balancer with explicit mappings of metadata to particular ranks. This can allow
the administrator or users to evenly spread application load or limit impact of
users' metadata requests on the entire cluster.

The mechanism provided for this purpose is called an ``export pin``, an
extended attribute of directories. The name of this extended attribute is
``ceph.dir.pin``.  Users can set this attribute using standard commands:

.. prompt:: bash #

    setfattr -n ceph.dir.pin -v 2 path/to/dir

The value of the extended attribute is the rank to assign the directory subtree
to. A default value of ``-1`` indicates the directory is not pinned.

A directory's export pin is inherited from its closest parent with a set export
pin.  In this way, setting the export pin on a directory affects all of its
children. However, the parents pin can be overridden by setting the child
directory's export pin. For example:

.. prompt:: bash #

    mkdir -p a/b
    # "a" and "a/b" both start without an export pin set
    setfattr -n ceph.dir.pin -v 1 a/
    # a and b are now pinned to rank 1
    setfattr -n ceph.dir.pin -v 0 a/b
    # a/b is now pinned to rank 0 and a/ and the rest of its children are still pinned to rank 1


.. _cephfs-ephemeral-pinning:

Setting subtree partitioning policies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is also possible to setup **automatic** static partitioning of subtrees via
a set of **policies**. In CephFS, this automatic static partitioning is
referred to as **ephemeral pinning**. Any directory (inode) which is
ephemerally pinned will be automatically assigned to a particular rank
according to a consistent hash of its inode number. The set of all
ephemerally pinned directories should be uniformly distributed across all
ranks.

Ephemerally pinned directories are so named because the pin may not persist
once the directory inode is dropped from cache. However, an MDS failover does
not affect the ephemeral nature of the pinned directory. The MDS records what
subtrees are ephemerally pinned in its journal so MDS failovers do not drop
this information.

A directory is either ephemerally pinned or not. Which rank it is pinned to is
derived from its inode number and a consistent hash. This means that
ephemerally pinned directories are somewhat evenly spread across the MDS
cluster. The **consistent hash** also minimizes redistribution when the MDS
cluster grows or shrinks. So, growing an MDS cluster may automatically increase
your metadata throughput with no other administrative intervention.

Presently, there are two types of ephemeral pinning:

**Distributed Ephemeral Pins**: This policy causes a directory to fragment
(even well below the normal fragmentation thresholds) and distribute its
fragments as ephemerally pinned subtrees. This has the effect of distributing
immediate children across a range of MDS ranks.  The canonical example use-case
would be the ``/home`` directory: we want every user's home directory to be
spread across the entire MDS cluster. This can be set via:

.. prompt:: bash #

    setfattr -n ceph.dir.pin.distributed -v 1 /cephfs/home


**Random Ephemeral Pins**: This policy indicates any descendent sub-directory
may be ephemerally pinned. This is set through the extended attribute
``ceph.dir.pin.random`` with the value set to the percentage of directories
that should be pinned. For example:

.. prompt:: bash #

    setfattr -n ceph.dir.pin.random -v 0.5 /cephfs/tmp

Would cause any directory loaded into cache or created under ``/tmp`` to be
ephemerally pinned 50 percent of the time.

It is recommended to only set this to small values, like ``.001`` or ``0.1%``.
Having too many subtrees may degrade performance. For this reason, the config
``mds_export_ephemeral_random_max`` enforces a cap on the maximum of this
percentage (default: ``.01``). The MDS returns ``EINVAL`` when attempting to
set a value beyond this config.

Both random and distributed ephemeral pin policies are off by default in
Octopus. The features may be enabled via the
``mds_export_ephemeral_random`` and ``mds_export_ephemeral_distributed``
configuration options.

Ephemeral pins may override parent export pins and vice versa. What determines
which policy is followed is the rule of the closest parent: if a closer parent
directory has a conflicting policy, use that one instead. For example:

.. prompt:: bash #

    mkdir -p foo/bar1/baz foo/bar2
    setfattr -n ceph.dir.pin -v 0 foo
    setfattr -n ceph.dir.pin.distributed -v 1 foo/bar1

The ``foo/bar1/baz`` directory will be ephemerally pinned because the
``foo/bar1`` policy overrides the export pin on ``foo``. The ``foo/bar2``
directory will obey the pin on ``foo`` normally.

For the reverse situation:

.. prompt:: bash #

    mkdir -p home/{patrick,john}
    setfattr -n ceph.dir.pin.distributed -v 1 home
    setfattr -n ceph.dir.pin -v 2 home/patrick

The ``home/patrick`` directory and its children will be pinned to rank 2
because its export pin overrides the policy on ``home``.

To remove a partitioning policy, remove the respective extended attribute
or set the value to 0.

.. prompt:: bash #

   $ setfattr -n ceph.dir.pin.distributed -v 0 home
   # or
   $ setfattr -x ceph.dir.pin.distributed home

For export pins, remove the extended attribute or set the extended attribute
value to `-1`.

.. prompt:: bash #

   $ setfattr -n ceph.dir.pin -v -1 home


Dynamic Subtree Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CephFS has long had a dynamic metadata blanacer (sometimes called the "default
balancer") which can split or merge subtrees while placing them on "colder" MDS
ranks. Moving the metadata around can improve overall file system throughput
and cache size.

However, the balancer has suffered from problem with efficiency and performance
so it is by default turned off. This is to avoid an administrator "turning on
multimds" by increasing the ``max_mds`` setting and then finding the balancer
has made a mess of the cluster performance (reverting is straightforward but
can take time).

The setting to turn on the balancer is:

.. prompt:: bash #

   ceph fs set <fs_name> balance_automate true

Turning on the balancer should only be done with appropriate configuration,
such as with the ``bal_rank_mask`` setting (described below). Careful
monitoring of the file system performance and MDS is advised.


Dynamic subtree partitioning with Balancer on specific ranks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CephFS file system provides the ``bal_rank_mask`` option to enable the balancer
to dynamically rebalance subtrees within particular active MDS ranks. This
allows administrators to employ both the dynamic subtree partitioning and
static pining schemes in different active MDS ranks so that metadata loads
are optimized based on user demand. For instance, in realistic cloud
storage environments, where a lot of subvolumes are allotted to multiple
computing nodes (e.g., VMs and containers), some subvolumes that require
high performance are managed by static partitioning, whereas most subvolumes
that experience a moderate workload are managed by the balancer. As the balancer
evenly spreads the metadata workload to all active MDS ranks, performance of
static pinned subvolumes inevitably may be affected or degraded. If this option
is enabled, subtrees managed by the balancer are not affected by
static pinned subtrees.

This option can be configured with the ``ceph fs set`` command. For example:

.. prompt:: bash #

    ceph fs set <fs_name> bal_rank_mask <hex> 

Each bitfield of the ``<hex>`` number represents a dedicated rank. If the ``<hex>`` is
set to ``0x3``, the balancer runs on active ``0`` and ``1`` ranks. For example:

.. prompt:: bash #

    ceph fs set <fs_name> bal_rank_mask 0x3

If the ``bal_rank_mask`` is set to ``-1`` or ``all``, all active ranks are masked
and utilized by the balancer. As an example:

.. prompt:: bash #

    ceph fs set <fs_name> bal_rank_mask -1

On the other hand, if the balancer needs to be disabled,
the ``bal_rank_mask`` should be set to ``0x0``. For example:

.. prompt:: bash #

    ceph fs set <fs_name> bal_rank_mask 0x0
