.. _require_osd_release:

=====================
 Minimum OSD Release
=====================

The ``require_osd_release`` field of the OSDMap is the cluster-wide
declaration that every OSD is running at least the named release. It is
set by the operator at the tail of a cluster upgrade, once all OSDs have
been upgraded to the new binary. Raising it unlocks features that would
otherwise be rejected as unsafe, and it bounds how far ahead of the
cluster a new OSD release is allowed to boot.

Unlike :ref:`require_min_compat_client`, which can be lowered when
features in use permit, ``require_osd_release`` can only move forward.

Setting the flag
================

.. prompt:: bash $

   ceph osd require-osd-release <release>

For example, after finishing an upgrade to *tentacle*:

.. prompt:: bash $

   ceph osd require-osd-release tentacle

Monitors only accept a small set of recent release names. The current
accepted targets are ``squid``, ``tentacle``, and ``umbrella``; any
older release name is rejected with ``not supported for this release``.
The accepted set slides forward as new Ceph releases are added.

The target release must be in the accepted set and must not be older
than the current value; setting it to the current value is a no-op.
Additionally, the Monitors verify that every Monitor in the quorum and
every up OSD advertises the target release's feature bits. The Monitor
feature check has no bypass; the OSD feature check can be overridden
with ``--yes-i-really-mean-it``, but any OSD that lacks the feature
will still be refused when it tries to boot (see `OSD boot window`_).

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

An OSD refuses to boot if it is running a release that is more than
two ahead of ``require_osd_release``. For example, with
``require_osd_release`` set to ``reef``:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - OSD release
     - Boots?
     - Notes
   * - ``reef``
     - yes
     - Same release.
   * - ``squid``
     - yes
     - One release ahead.
   * - ``tentacle``
     - yes
     - Two releases ahead (at the edge of the window).
   * - ``umbrella``
     - **no**
     - ``disallowing boot of umbrella+ OSD ... because
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

The appearance of this warning is the normal cue to run
``ceph osd require-osd-release <release>``; the warning clears once the
flag has been raised.

Initial value on new clusters
=============================

New clusters start with ``require_osd_release`` set to the newest
release the Monitor code knows about. On this release that is
``umbrella``. Two developer options adjust the initial value for
testing; neither should be used in production.

.. confval:: mon_debug_no_require_umbrella
.. confval:: mon_debug_no_require_tentacle

Setting ``mon_debug_no_require_umbrella`` alone downshifts the initial
value to ``tentacle``; setting both flags downshifts it further to
``squid``. The ``tentacle`` flag has no effect unless the umbrella flag
is also set.
