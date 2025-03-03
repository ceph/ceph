
===========================
Compatibility and Stability
===========================

.. _cephadm-compatibility-with-podman:

Compatibility with Podman Versions
----------------------------------

Podman and Ceph have different end-of-life strategies. This means that care
must be taken in finding a version of Podman that is compatible with Ceph.

This table shows which version pairs are expected to work or not work together:


+-----------+-----------------------------------------------+
|  Ceph     |                 Podman                        |
+-----------+-------+-------+-------+-------+-------+-------+
|           | 1.9   |  2.0  |  2.1  |  2.2  |  3.0  | > 3.0 |
+===========+=======+=======+=======+=======+=======+=======+
| <= 15.2.5 | True  | False | False | False | False | False |
+-----------+-------+-------+-------+-------+-------+-------+
| >= 15.2.6 | True  | True  | True  | False | False | False |
+-----------+-------+-------+-------+-------+-------+-------+
| >= 16.2.1 | False | True  | True  | False | True  | True  |
+-----------+-------+-------+-------+-------+-------+-------+
| >= 17.2.0 | False | True  | True  | False | True  | True  |
+-----------+-------+-------+-------+-------+-------+-------+

.. note::

  While not all Podman versions have been actively tested against
  all Ceph versions, there are no known issues with using Podman
  version 3.0 or greater with Ceph Quincy and later releases.

.. warning:: 

   To use Podman with Ceph Pacific, you must use **a version of Podman that
   is 2.0.0 or higher**. However, **Podman version 2.2.1 does not work with
   Ceph Pacific**.
   
   "Kubic stable" is known to work with Ceph Pacific, but it must be run
   with a newer kernel.


.. _cephadm-stability:

Stability
---------

Cephadm is relatively stable but new functionality is still being
added and bugs are occasionally discovered. If issues are found, please
open a tracker issue under the Orchestrator component (https://tracker.ceph.com/projects/orchestrator/issues)

Cephadm support remains under development for the following features:

- ceph-exporter deployment
- stretch mode integration
- monitoring stack (moving towards prometheus service discover and providing TLS)
- RGW multisite deployment support (requires lots of manual steps currently)
- cephadm agent

If a cephadm command fails or a service stops running properly, see
:ref:`cephadm-pause` for instructions on how to pause the Ceph cluster's
background activity and how to disable cephadm.
