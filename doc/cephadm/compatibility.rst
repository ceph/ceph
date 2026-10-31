.. _cephadm-compatibility:

=============================
 Compatibility and Stability
=============================

.. meta::
   :description: The Podman versions that work with each Ceph release, and the features for which cephadm support is not complete.
   :ceph-page-type: reference

The Podman versions that work with each Ceph release, and the features for
which :term:`cephadm` support is not complete.

.. _cephadm-compatibility-with-podman:

Compatibility with Podman Versions
==================================

Use the last row that matches your Ceph release.

+-----------+-----------------------------------------------+
|  Ceph     |                 Podman                        |
+-----------+-------+-------+-------+-------+-------+-------+
|           | 1.9   |  2.0  |  2.1  |  2.2  |  3.0  | > 3.0 |
+===========+=======+=======+=======+=======+=======+=======+
| <= 15.2.5 | True  | False | False | False | False | False |
+-----------+-------+-------+-------+-------+-------+-------+
| >= 15.2.6 | True  | True  | True  | False | False | False |
+-----------+-------+-------+-------+-------+-------+-------+
| >= 16.2.1 | False | 2.0.2+| True  | False | True  | True  |
+-----------+-------+-------+-------+-------+-------+-------+
| >= 17.2.0 | False | 2.0.2+| True  | False | True  | True  |
+-----------+-------+-------+-------+-------+-------+-------+

- ``True`` and ``False`` say whether the pair is expected to work. Podman
  and Ceph have different end-of-life strategies, and not every Podman
  version has been tested with every Ceph release.
- ``2.0.2+`` means Podman 2.0.2 or a later 2.0 release. Since Pacific
  (16.2.0), ``cephadm`` rejects Podman older than 2.0.2: most ``cephadm``
  commands stop with the error
  ``podman version 2.0.2 or later is required``.
- Podman 2.2.1 does not work with Ceph Pacific.
- "Kubic stable" is known to work with Ceph Pacific, but it must be run with
  a newer kernel.
- The SMB service needs Podman 4.1.0 or later (see
  :ref:`deploy-cephadm-smb-samba`). Ubuntu 22.04 ships Podman 3.4.4, so the
  SMB service cannot run there with the distribution's Podman.
- ``cephadm`` does not check the version of Docker.

.. _cephadm-stability:

Features with Incomplete Cephadm Support
========================================

Cephadm is relatively stable, but new functionality is still being added and
bugs are occasionally discovered. Cephadm support is not complete for the
following features:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Feature
     - State
   * - Cephadm agent
     - An agent on each host that gathers and sends host metadata. Disabled
       by default (``mgr/cephadm/use_agent``).
   * - Monitoring stack
     - Prometheus HTTP service discovery is in place. TLS for the monitoring
       daemons is disabled by default
       (``mgr/cephadm/secure_monitoring_stack``). See
       :ref:`mgr-cephadm-monitoring`.
   * - :term:`RGW` multisite
     - Cephadm deploys the gateways but does not create the realm,
       zonegroup, or zone. Use the ``rgw`` Manager module for that: see
       :ref:`mgr-rgw-module`.
   * - SMB service
     - Under active development; many features may be missing or immature.
       See :ref:`deploy-cephadm-smb-samba`.
   * - Stretch mode
     - Cephadm sets the :term:`CRUSH` locations of Monitors from the service
       specification, which is the recommended way to replace a tiebreaker
       Monitor, but it does not enable stretch mode. See :ref:`stretch_mode`.

If a cephadm command fails or a service stops running properly, see
:ref:`cephadm-pause` for how to pause the Ceph cluster's background activity
and how to disable cephadm. Report bugs in the `Orchestrator issue tracker
<https://tracker.ceph.com/projects/orchestrator/issues>`_.

Additional Resources
====================

- :ref:`Cephadm host requirements <cephadm-host-requirements>`
- :ref:`os-recommendations`
