
===========================
Compatibility and Stability
===========================

.. _cephadm-compatibility-with-podman:

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

.. warning:: 
  Only podman versions that are 2.0.0 and higher work with Ceph Pacific, with the exception of podman version 2.2.1, which does not work with Ceph Pacific. kubic stable is known to work with Ceph Pacific, but it must be run with a newer kernel.


.. _cephadm-stability:

Stability
---------

Cephadm is actively in development. Please be aware that some
functionality is still rough around the edges. Especially the 
following components are working with cephadm, but the
documentation is not as complete as we would like, and there may be some
changes in the near future:

- RGW

Cephadm support for the following features is still under development and may see breaking
changes in future releases:

- Ingress
- Cephadm exporter daemon
- cephfs-mirror

In case you encounter issues, see also :ref:`cephadm-pause`.
