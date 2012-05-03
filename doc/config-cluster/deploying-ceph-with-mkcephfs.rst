================================
Deploying Ceph with ``mkcephfs``
================================

Once you have copied your Ceph Configuration to the OSD Cluster hosts, you may deploy Ceph with the ``mkcephfs`` script.

.. note::  ``mkcephfs`` is a quick bootstrapping tool. It does not handle more complex operations, such as upgrades.

	For production environments, you will deploy Ceph using Chef cookbooks (coming soon!).

To run ``mkcephfs``, execute the following::

	$ mkcephfs -a -c <path>/ceph.conf -k mycluster.keyring

The script adds an admin key to the ``mycluster.keyring``, which is analogous to a root password.

To start the cluster, execute the following::

	/etc/init.d/ceph -a start

Ceph should begin operating. You can check on the health of your Ceph cluster with the following::

	ceph -k mycluster.keyring -c <path>/ceph.conf health
