==============================
 Resizing the monitor cluster
==============================

.. _adding-mon:

Adding a monitor
----------------

#. Initialize the new monitor's data directory with the ``ceph-mon
   --mkfs`` command.  You need to provide the new monitor with three
   pieces of information:

   - the cluster fsid.  This can come from a monmap (``--monmap
     </path/to/monmap>``) for explicitly via ``--fsid <fsid>``.
   - one or more existing monitors to join.  This can come via ``-m
     <host1,host2,...>``, a monmap (``--monmap </some/path>``), or
     ``[mon.foo]`` sections with ``mon addr`` fields in *ceph.conf*.
   - the monitor authentication key ``mon.``.  This should be passed
     in explicitly via a keyring (``--keyring </some/path>``).

   Any combination of the above arguments that provide the four needed
   pieces of information will work.  The simplest way to do this is
   usually::

     $ ceph mon getmap -o /tmp/monmap           # provides fsid and existing monitor addrs
     $ ceph auth get mon. -o /tmp/monkey        # mon. auth key
     $ ceph-mon -i newname --mkfs --monmap /tmp/monmap --keyring /tmp/monkey

#. Start the new monitor and it will automatically join the cluster.
   The daemon needs to know which address to bind to, either via
   ``--public-addr <ip:port>`` or by setting ``mon addr`` in the
   appropriate section of *ceph.conf*.  For example::

    $ ceph-mon -i newname --public-addr <ip:port>

#. If you would like other nodes to be able to use this monitor during
   their initial startup, you'll need to adjust *ceph.conf* to add a
   section and ``mon addr`` for the new monitor, or add it to the
   existing ``mon host`` list.

Removing a monitor from a healthy cluster
-----------------------------------------

If the cluster is healthy, you can do::

  $ ceph mon remove $id

For example, if your cluster includes ``mon.a``, ``mon.b``, and ``mon.c``, then you can remove ``mon.c`` with::

  $ ceph mon remove c

Removing a monitor from an unhealthy or down cluster
----------------------------------------------------

The mon cluster may not be up because you have lost too many nodes to
form a quorum.

#) On a surviving monitor node, find the most recent monmap::

     $ ls $mon_data/monmap
     1  2  accepted_pn  last_committed  latest

   in this case it is 2.

#) Copy to a temporary location and modify the monmap to remove the
   node(s) you don't want.  Let's say the map has ``mon.a``, ``mon.b``,
   and ``mon.c``, but only ``mon.a`` is surviving::

     $ cp $mon_data/monmap/2 /tmp/foo
     $ monmaptool /tmp/foo --rm b
     $ monmaptool /tmp/foo --rm c

3) Make sure ceph-mon isn't running::

     $ service ceph stop mon

4) Inject the modified map on any surviving nodes.  For example, for
   ``mon.a``::

     $ ceph-mon -i a --inject-monmap /tmp/foo   # for each surviving monitor

5) Start the surviving monitor(s)::

     $ service ceph start mon           # on each node with a surviving monitor

6) Remove the old monitors from *ceph.conf* so that nobody tries to
   connect to the old instances.
