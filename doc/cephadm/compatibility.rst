
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

Cephadm is actively in development. Please be aware that some
functionality is still rough around the edges. Especially the 
following components are working with cephadm, but the
documentation is not as complete as we would like, and there may be some
changes in the near future:

- RGW

Cephadm support for the following features is still under development:

- RGW-HA
- Cephadm exporter daemon
- cephfs-mirror

In case you encounter issues, see also :ref:`cephadm-pause`.

