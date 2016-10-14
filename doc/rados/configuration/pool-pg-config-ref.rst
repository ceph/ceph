======================================
 Pool, PG and CRUSH Config Reference
======================================

.. index:: pools; configuration

When you create pools and set the number of placement groups for the pool, Ceph
uses default values when you don't specifically override the defaults. **We
recommend** overridding some of the defaults. Specifically, we recommend setting
a pool's replica size and overriding the default number of placement groups. You
can specifically set these values when running `pool`_ commands. You can also
override the defaults by adding new ones in the ``[global]`` section of  your
Ceph configuration file. 


.. literalinclude:: pool-pg.conf
   :language: ini



``mon max pool pg num``

:Description: The maximum number of placement groups per pool.
:Type: Integer
:Default: ``65536``


``mon pg create interval`` 

:Description: Number of seconds between PG creation in the same 
              Ceph OSD Daemon.

:Type: Float
:Default: ``30.0``


``mon pg stuck threshold`` 

:Description: Number of seconds after which PGs can be considered as 
              being stuck.

:Type: 32-bit Integer
:Default: ``300``


``osd pg bits`` 

:Description: Placement group bits per Ceph OSD Daemon.
:Type: 32-bit Integer
:Default: ``6`` 


``osd pgp bits`` 

:Description: The number of bits per Ceph OSD Daemon for PGPs.
:Type: 32-bit Integer
:Default: ``6``


``osd crush chooseleaf type``

:Description: The bucket type to use for ``chooseleaf`` in a CRUSH rule. Uses 
              ordinal rank rather than name.

:Type: 32-bit Integer
:Default: ``1``. Typically a host containing one or more Ceph OSD Daemons.


``osd crush initial weight``

:Description: The initial crush weight for newly added osds into crushmap.

:Type: Double
:Default: ``the size of newly added osd in TB``. By default, the initial crush
          weight for the newly added osd is set to its volume size in TB.
          See `Weighting Bucket Items`_ for details.


``osd pool default crush replicated ruleset`` 

:Description: The default CRUSH ruleset to use when creating a replicated pool.
:Type: 8-bit Integer
:Default: ``CEPH_DEFAULT_CRUSH_REPLICATED_RULESET``, which means "pick
          a ruleset with the lowest numerical ID and use that".  This is to
          make pool creation work in the absence of ruleset 0.


``osd pool erasure code stripe width`` 

:Description: Sets the desired size, in bytes, of an object stripe on every
              erasure coded pools. Every object if size S will be stored as 
              N stripes and each stripe will be encoded/decoded individually.

:Type: Unsigned 32-bit Integer
:Default: ``4096`` 


``osd pool default size``

:Description: Sets the number of replicas for objects in the pool. The default
              value is the same as
              ``ceph osd pool set {pool-name} size {size}``.

:Type: 32-bit Integer
:Default: ``3``


``osd pool default min size``

:Description: Sets the minimum number of written replicas for objects in the 
             pool in order to acknowledge a write operation to the client. 
             If minimum is not met, Ceph will not acknowledge the write to the 
             client. This setting ensures a minimum number of replicas when 
             operating in ``degraded`` mode.

:Type: 32-bit Integer
:Default: ``0``, which means no particular minimum. If ``0``, 
          minimum is ``size - (size / 2)``.


``osd pool default pg num`` 

:Description: The default number of placement groups for a pool. The default 
              value is the same as ``pg_num`` with ``mkpool``.

:Type: 32-bit Integer
:Default: ``8`` 


``osd pool default pgp num`` 

:Description: The default number of placement groups for placement for a pool. 
              The default value is the same as ``pgp_num`` with ``mkpool``. 
              PG and PGP should be equal (for now).

:Type: 32-bit Integer
:Default: ``8``


``osd pool default flags``

:Description: The default flags for new pools. 
:Type: 32-bit Integer
:Default: ``0``


``osd max pgls``

:Description: The maximum number of placement groups to list. A client 
              requesting a large number can tie up the Ceph OSD Daemon.

:Type: Unsigned 64-bit Integer
:Default: ``1024``
:Note: Default should be fine.


``osd min pg log entries`` 

:Description: The minimum number of placement group logs to maintain 
              when trimming log files.

:Type: 32-bit Int Unsigned
:Default: ``1000``


``osd default data pool replay window``

:Description: The time (in seconds) for an OSD to wait for a client to replay
              a request.

:Type: 32-bit Integer
:Default: ``45``


.. _pool: ../../operations/pools
.. _Monitoring OSDs and PGs: ../../operations/monitoring-osd-pg#peering
.. _Weighting Bucket Items: ../../operations/crush-map#weightingbucketitems
