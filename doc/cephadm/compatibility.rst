
===========================
Compatibility and Stability
===========================

Compatibility with Podman Versions
----------------------------------

Podman and Ceph have different end-of-life strategies that
might make it challenging to find compatible Podman and Ceph 
versions

Those versions are expected to work:


+-----------+---------------------------------------+
|  Ceph     |                 Podman                |
+-----------+-------+-------+-------+-------+-------+
|           | 1.9   |  2.0  |  2.1  |  2.2  |  3.0  |
+===========+=======+=======+=======+=======+=======+
| <= 15.2.5 | True  | False | False | False | False |
+-----------+-------+-------+-------+-------+-------+
| >= 15.2.6 | True  | True  | True  | False | False |
+-----------+-------+-------+-------+-------+-------+
| >= 16.2.1 | False | True  | True  | False | True  |
+-----------+-------+-------+-------+-------+-------+

.. _cephadm-stability:

Stability
---------

Cephadm is a new feature in the Octopus release and has seen limited
use in production and at scale.  We would like users to try cephadm,
especially for new clusters, but please be aware that some
functionality is still rough around the edges.  We expect fairly
frequent updates and improvements over the first several bug fix
releases of Octopus.

Cephadm management of the following components are currently well-supported:

- Monitors
- Managers
- OSDs
- CephFS file systems
- rbd-mirror
- NFS
- iSCSI

The following components are working with cephadm, but the
documentation is not as complete as we would like, and there may be some
changes in the near future:

- RGW

Cephadm support for the following features is still under development:

- RGW-HA

If you run into problems, you can always pause cephadm with::

  ceph orch pause

Or turn cephadm off completely with::

  ceph orch set backend ''
  ceph mgr module disable cephadm

