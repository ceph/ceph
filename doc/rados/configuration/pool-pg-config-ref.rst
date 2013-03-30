======================================
 Pool, PG and CRUSH Config Reference
======================================

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

:Description: The maximium number of placement groups per pool.
:Type: Integer
:Default: ``65536``


``mon pg create interval`` 

:Description: Number of seconds between PG creation in the same OSD.
:Type: Float
:Default: ``30.0``


``mon pg stuck threshold`` 

:Description: Number of seconds after which PGs can be considered as being stuck.
:Type: 32-bit Integer
:Default: ``300``


``osd pg bits`` 

:Description: Placement group bits per OSD.
:Type: 32-bit Integer
:Default: ``6`` 


``osd pgp bits`` 

:Description: The number of bits per OSD for PGPs.
:Type: 32-bit Integer
:Default: ``6``


``osd crush chooseleaf type``

:Description: The bucket type to use for ``chooseleaf`` in a CRUSH rule. Uses 
              ordinal rank rather than name.

:Type: 32-bit Integer
:Default: ``1``. Typically a host containing one or more OSDs.


``osd min rep``

:Description: The minimum number of replicas for a ruleset.
:Type: 32-bit Integer
:Default: ``1``


``osd max rep``

:Description: The maximum number of replicas for a ruleset.
:Type: 32-bit Integer
:Default: ``10``


``osd pool default crush rule`` 

:Description: The default CRUSH ruleset to use when creating a pool.
:Type: 32-bit Integer
:Default: ``0``


``osd pool default size`` 

:Description: Sets the number of replicas for objects in the pool. The default 
              value is the same as 
              ``ceph osd pool set {pool-name} size {size}``.

:Type: 32-bit Integer
:Default: ``2`` 


``osd pool default min size``

:Descrption: Sets the minimum number of written replicas for objects in the 
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
              requesting a large number can tie up the OSD.

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
