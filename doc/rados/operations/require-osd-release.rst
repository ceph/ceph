.. _require_osd_release:

=====================
 Minimum OSD Release
=====================

The ``require_osd_release`` field of the OSDMap is the cluster-wide
declaration that every OSD is running at least the named release. It is
raised at the tail of a cluster upgrade, once all OSDs have been upgraded
to the new release. cephadm (``ceph orch upgrade``) and Rook raise it
automatically; on other clusters, the operator raises it. Raising it unlocks
features that would otherwise be rejected as unsafe, and it bounds how far
ahead of the cluster a new OSD release is allowed to boot.

Unlike :ref:`require_min_compat_client`, which can be lowered when
features in use permit, ``require_osd_release`` can only move forward.

Setting the flag
================

.. prompt:: bash $

   ceph osd require-osd-release <release>

For example, after finishing an upgrade to *tentacle*:

.. prompt:: bash $

   ceph osd require-osd-release tentacle

Monitors only accept a small set of recent release names. As of 2026 November,
the accepted targets are ``squid``, ``tentacle``, and ``umbrella``; any older
release name is rejected.
The accepted set slides forward as new Ceph releases are added.

The target release must be in the accepted set and must not be older
than the current value; setting it to the current value is a no-op.
Additionally, the Monitors verify that every Monitor in the quorum and
every up OSD advertises the target release's feature bits. The Monitor
feature check has no bypass; the OSD feature check can be overridden
with ``--yes-i-really-mean-it``, but any OSD that lacks the feature
will still be refused when it tries to boot (see `OSD boot window`_).

The Monitors also refuse the command, unless you pass
``--yes-i-really-mean-it``, when the current value is older than ``squid``
or when no OSD is ``up``.

Checking the current value
==========================

.. prompt:: bash $

   ceph osd dump | grep require_osd_release

What raising the flag enables
=============================

Raising ``require_osd_release`` unlocks a set of commands and features
that the monitor refuses while the flag is below the required level.

.. list-table::
   :header-rows: 1
   :widths: 25 70

   * - Required release
     - Commands or features unlocked
   * - ``luminous``
     - ``ceph osd crush set-device-class``, ``ceph osd crush
       rm-device-class``
   * - ``nautilus``
     - ``pg_autoscale_mode``, ``target_size_bytes``, ``target_size_ratio``,
       new-format ``pg_num`` changes
   * - ``tentacle``
     - ``allow_ec_optimizations`` pool flag

The gates are cumulative: a cluster whose ``require_osd_release`` is at
``squid`` or higher satisfies the ``luminous`` and ``nautilus`` gates
automatically. Because the current accepted-target set does not include
releases older than ``squid``, the ``luminous`` and ``nautilus`` rows
are historical on modern clusters and are listed for completeness.

OSD boot window
===============

The Monitors refuse the boot of an OSD whose release is more than two
releases ahead of ``require_osd_release``. Umbrella Monitors also refuse any
OSD older than ``squid``. For example, on a cluster whose Monitors run
Umbrella and whose ``require_osd_release`` is ``reef``:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - OSD release
     - Boots?
     - Notes
   * - ``reef``
     - **no**
     - ``disallowing boot of OSD osd.3 v2:10.0.0.3:6802/1234 because the osd
       lacks CEPH_FEATURE_SERVER_SQUID``
   * - ``squid``
     - yes
     - One release ahead.
   * - ``tentacle``
     - yes
     - Two releases ahead (at the edge of the window).
   * - ``umbrella``
     - **no**
     - ``disallowing boot of umbrella+ OSD osd.3 v2:10.0.0.3:6802/1234 because
       require_osd_release < squid``

This sliding window forces operators to raise ``require_osd_release``
roughly every two releases, keeping the cluster's OSD code span
bounded.

.. _OSD_UPGRADE_FINISHED:

Health warning: ``OSD_UPGRADE_FINISHED``
========================================

If all up OSDs advertise the ``SERVER_<RELEASE>`` feature but
``require_osd_release`` is still below that release, the Monitors emit
a ``HEALTH_WARN`` check ``OSD_UPGRADE_FINISHED`` with the message::

   all OSDs are running <release> or later but require_osd_release < <release>

On a cluster where no orchestrator raises the flag, the appearance of this
warning is a cue to run ``ceph osd require-osd-release <release>``. The
warning clears once the flag has been raised.

Initial value on new clusters
=============================

New clusters start with ``require_osd_release`` set to the newest
release the Monitor code knows about. For Umbrella Monitors, that is
``umbrella``. Two developer options adjust the initial value for
testing; neither should be used in production.

.. confval:: mon_debug_no_require_umbrella
.. confval:: mon_debug_no_require_tentacle

Setting ``mon_debug_no_require_umbrella`` alone downshifts the initial
value to ``tentacle``; setting both flags downshifts it further to
``squid``. The ``tentacle`` flag has no effect unless the umbrella flag
is also set.
