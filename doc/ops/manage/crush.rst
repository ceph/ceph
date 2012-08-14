.. _adjusting-crush:

=========================
 Adjusting the CRUSH map
=========================

There are a few ways to adjust the crush map:

* online, by issuing commands to the monitor
* offline, by extracting the current map to a file, modifying it, and then reinjecting a new map

For offline changes, some can be made directly with ``crushtool``, and
others require you to decompile the file to text form, manually edit
it, and then recompile.


Adding a new device (OSD) to the map
====================================

.. _adjusting-crush-set:

Adding new devices or moving existing devices to new positions in the
CRUSH hierarchy can be done via the monitor.  The general form is::

  $ ceph osd crush set <id> <name> <weight> [<loc> [<lo2> ...]]

where

  * ``id`` is the numeric device id (the OSD id)
  * ``name`` is an alphanumeric name.  By convention Ceph uses
    ``osd.$id``.
  * ``weight`` is a floating point weight value controlling how much
    data the device will be allocated.  A decent convention is to make
    this the number of TB the device will store.
  * ``loc`` is a list of ``what=where`` pairs indicating where in the
    CRUSH hierarchy the device will be stored.  By default, the
    hierarchy (the ``what``s) includes ``pool`` (the ``default`` pool
    is normally the root of the hierarchy), ``rack``, and ``host``.
    At least one of these location specifiers has to refer to an
    existing point in the hierarchy, and only the lowest (most
    specific) match counts.  Beneath that point, any intervening
    branches will be created as needed.  Specifying the complete
    location is always sufficient, and also safe in that existing
    branches (and devices) won't be moved around.

For example, if the OSD id is ``123``, we want a weight of ``1.0`` and
the device is on host ``hostfoo`` and rack ``rackbar``::

   $ ceph osd crush set 123 osd.123 1.0 pool=default rack=rackbar host=hostfoo

will add it to the hierarchy, or move it from its previous position.
The rack ``rackbar`` and host ``hostfoo`` will be added as needed, as
long as the pool ``default`` exists (as it does in the default Ceph
CRUSH map generated during cluster creation).

Note that if I later add another device in the same host but specify a
different pool or rack::

   $ ceph osd crush set 124 osd.124 1.0 pool=nondefault rack=weirdrack host=hostfoo

the device will still be placed in host ``hostfoo`` at its current
location (rack ``rackbar`` and pool ``default``).


Moving a bucket to a different position in the hierarchy
========================================================

To move an existing bucket to a different position in the hierarchy,
identify the bucket to move by name and specify the new location in
the same fashion as with ``osd crush set ...``::

  $ ceph osd crush move <bucket name> [<loc> [<loc2> ...]]

where

  * ``name`` is the name of the bucket to move.  (To move a device,
    see :ref:`adjusting-crush-set`.)
  * ``loc`` is a list of ``what=where`` pairs indicating where the bucket should
    be moved.  See :ref:`adjusting-crush-set`.


Adjusting the CRUSH weight
==========================

You can adjust the CRUSH weight for a device with::

   $ ceph osd crush reweight osd.123 2.0

Removing a device
=================

You can remove a device from the crush map with::

   $ ceph osd crush remove osd.123

Tunables
========

There are several magic numbers that were used in the original CRUSH
implementation that have proven to be poor choices.  To support
the transition away from them, newer versions of CRUSH (starting with
the v0.48 argonaut series) allow the values to be adjusted or tuned.

Clusters running recent Ceph releases support using the tunable values
in the CRUSH maps.  However, older clients and daemons will not correctly interact
with clusters using the "tuned" CRUSH maps.  To detect this situation,
there is now a feature bit ``CRUSH_TUNABLES`` (value 0x40000) to
reflect support for tunables.

If the OSDMap currently used by the ``ceph-mon`` or ``ceph-osd``
daemon has non-legacy values, it will require the ``CRUSH_TUNABLES``
feature bit from clients and daemons who connect to it.  This means
that old clients will not be able to connect.

At some future point in time, newly created clusters will have
improved default values for the tunables.  This is a matter of waiting
until the support has been present in the Linux kernel clients long
enough to make this a painless transition for most users.

Impact of legacy values
~~~~~~~~~~~~~~~~~~~~~~~

The legacy values result in several misbehaviors:

 * For hiearchies with a small number of devices in the leaf buckets,
   some PGs map to fewer than the desired number of replicas.  This
   commonly happens for hiearchies with "host" nodes with a small
   number (1-3) of OSDs nested beneath each one.

 * For large clusters, some small percentages of PGs map to less than
   the desired number of OSDs.  This is more prevalent when there are
   several layers of the hierarchy (e.g., row, rack, host, osd).

 * When some OSDs are marked out, the data tends to get redistributed
   to nearby OSDs instead of across the entire hierarchy.

Which client versions support tunables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 * argonaut series, v0.48.1 or later
 * v0.49 or later
 * Linux kernel version v3.5 or later (for the file system and RBD kernel clients)

A few important points
~~~~~~~~~~~~~~~~~~~~~~

 * Adjusting these values will result in the shift of some PGs between
   storage nodes.  If the Ceph cluster is already storing a lot of
   data, be prepared for some fraction of the data to move.
 * The ``ceph-osd`` and ``ceph-mon`` daemons will start requiring the
   ``CRUSH_TUNABLES`` feature of new connections as soon as they get
   the updated map.  However, already-connected clients are
   effectively grandfathered in, and will misbehave if they do not
   support the new feature.
 * If the CRUSH tunables are set to non-legacy values and then later
   changed back to the defult values, ``ceph-osd`` daemons will not be
   required to support the feature.  However, the OSD peering process
   requires examining and understanding old maps.  Therefore, you
   should not run old (pre-v0.48) versions of the ``ceph-osd`` daemon
   if the cluster has previosly used non-legacy CRUSH values, even if
   the latest version of the map has been switched back to using the
   legacy defaults.

Tuning CRUSH
~~~~~~~~~~~~

If you can ensure that all clients are running recent code, you can
adjust the tunables by extracting the CRUSH map, modifying the values,
and reinjecting it into the cluster.

 * Extract the latest CRUSH map::

   ceph osd getcrushmap -o /tmp/crush

 * Adjust tunables.  These values appear to offer the best behavior
   for both large and small clusters we tested with.  You will need to
   additionally specify the ``--enable-unsafe-tunables`` argument to
   ``crushtool`` for this to work.  Please use this option with
   extreme care.::

   crushtool -i /tmp/crush --set-choose-local-tries 0 --set-choose-local-fallback-tries 0 --set-choose-total-tries 50 -o /tmp/crush.new

 * Reinject modified map::

   ceph osd setcrushmap -i /tmp/crush.new

Legacy values
~~~~~~~~~~~~~

For reference, the legacy values for the CRUSH tunables can be set
with::

   crushtool -i /tmp/crush --set-choose-local-tries 2 --set-choose-local-fallback-tries 5 --set-choose-total-tries 19 -o /tmp/crush.legacy

Again, the special ``--enable-unsafe-tunables`` option is required.
Further, as noted above, be careful running old versions of the
``ceph-osd`` daemon after reverting to legacy values as the feature
bit is not perfectly enforced.

