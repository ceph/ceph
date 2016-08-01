===========================
 Quick Ceph Object Storage
===========================

To use the :term:`Ceph Object Storage` Quick Start guide, you must have executed the
procedures in the `Storage Cluster Quick Start`_ guide first. Make sure that you
have at least one :term:`RGW` instance running.

Configure new RGW instance
==========================

The :term:`RGW` instance created by the `Storage Cluster Quick Start`_ will run using
the embedded CivetWeb webserver. ``ceph-deploy`` will create the instance and start
it automatically with default parameters.

To administer the :term:`RGW` instance, see details in the the
`RGW Admin Guide`_.

Additional details may be found in the `Configuring Ceph Object Gateway`_ guide, but
the steps specific to Apache are no longer needed.

.. note:: Deploying RGW using ``ceph-deploy`` and using the CivetWeb webserver instead
   of Apache is new functionality as of **Hammer** release.


.. _Storage Cluster Quick Start: ../quick-ceph-deploy
.. _RGW Admin Guide: ../../radosgw/admin
.. _Configuring Ceph Object Gateway: ../../radosgw/config
