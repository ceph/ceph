.. _require_min_compat_client:

===========================
 Minimum Compatible Client
===========================

The ``require_min_compat_client`` field of the OSDMap declares the minimum
Ceph client release the cluster expects of its clients. It is an
operator-set hint that authorizes use of on-map features whose encoding
or semantics older clients cannot parse.

Setting the flag does not refuse older clients at connect time. Instead it
gates operator commands that would place feature bits into the OSDMap which
only newer clients understand. Once those features are in active use,
older clients can no longer parse the OSDMap; they lose the ability to
locate objects and will error or disconnect.

Setting the flag
================

.. prompt:: bash $

   ceph osd set-require-min-compat-client <release>

For example, to declare that the cluster requires at least *reef*:

.. prompt:: bash $

   ceph osd set-require-min-compat-client reef

Before committing the change, the Monitors inspect connected clients, MDSs,
and Managers. If any advertise release-feature bits that fall short of the
target, the command is rejected with a message like::

   cannot set require_min_compat_client to reef:
     3 connected client(s) look like luminous (missing 0x...)

Pass ``--yes-i-really-mean-it`` to proceed anyway. Those clients will be
disconnected once any feature the flag unlocks is actually enabled on the
map.

Checking the current value
==========================

.. prompt:: bash $

   ceph osd get-require-min-compat-client

or, for the full OSDMap context:

.. prompt:: bash $

   ceph osd dump | grep require_min_compat_client

To see which release each connected entity advertises:

.. prompt:: bash $

   ceph features

Lowering the flag
=================

Unlike :ref:`require_osd_release`, ``require_min_compat_client`` is not
monotonic. Monitors accept a lower value as long as no feature
currently on the OSDMap requires a higher one. The floor is computed from
features in active use on the map:

.. list-table::
   :header-rows: 1
   :widths: 55 25

   * - Feature in use
     - Minimum release
   * - CRUSH MSR rules
     - ``squid``
   * - ``pg-upmap-primary`` entries
     - ``reef``
   * - ``pg-upmap`` entries or CRUSH choose-args
     - ``luminous``
   * - CRUSH tunables level 5
     - ``jewel``
   * - CRUSH straw2 buckets
     - ``hammer``
   * - Per-OSD primary affinity, tunables level 3, or cache tiers
     - ``firefly``
   * - CRUSH tunables level 2 or ``OSDHASHPSPOOL`` pool flag
     - ``dumpling``
   * - CRUSH tunables (any non-legacy)
     - ``argonaut``

The last two rows are effectively always satisfied on modern clusters,
since ``OSDHASHPSPOOL`` has been set by default on every pool since
``dumpling`` and non-legacy CRUSH tunables have been the default since
``argonaut``. They are listed for completeness; in practice the floor
only ever sits at ``firefly`` or newer.

If the command fails and prints ``osdmap current utilizes features that
require <release>; cannot set require_min_compat_client below that``,
remove the feature that pins the floor (for example, clear all
``pg-upmap-primary`` entries) and retry.

Features gated by the flag
==========================

Raising the flag does not enable any feature on its own. It authorizes the
operator to run commands that would otherwise be rejected with an error
like ``min_compat_client luminous < reef, which is required for
pg-upmap-primary``:

.. list-table::
   :header-rows: 1
   :widths: 60 25

   * - Command
     - Minimum release
   * - | ``ceph osd primary-affinity``
       | ``ceph osd primary-temp``
       | ``ceph osd rm-primary-temp``
     - ``firefly``
   * - | ``ceph osd pg-upmap``
       | ``ceph osd rm-pg-upmap``
       | ``ceph osd pg-upmap-items``
       | ``ceph osd rm-pg-upmap-items``
       | ``ceph osd crush weight-set``
       | balancer ``upmap`` mode
     - ``luminous``
   * - | ``ceph osd pg-upmap-primary``
       | ``ceph osd rm-pg-upmap-primary``
       | ``ceph osd rm-pg-upmap-primary-all``
       | balancer ``read`` mode
       | balancer ``upmap-read`` mode
     - ``reef``

Note that adding CRUSH MSR rules is *not* gated at command time; however,
once an MSR rule is in the map, the features-in-use floor rises to
``squid`` (see the table above).

See :ref:`upmap` and :ref:`read_balancer` for the operational procedures
that depend on raising the flag. CephFS clients must satisfy both this
flag and any per-filesystem :ref:`cephfs_required_client_features` on
the file systems they mount.
