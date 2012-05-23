==================================
 Deploying Ceph with ``mkcephfs``
==================================

Once you have copied your Ceph Configuration to the OSD Cluster hosts,
you may deploy Ceph with the ``mkcephfs`` script.

.. note::  ``mkcephfs`` is a quick bootstrapping tool. It does not handle more 
           complex operations, such as upgrades.

For production environments, you deploy Ceph using Chef cookbooks. To run 
``mkcephfs``, execute the following:: 

   cd /etc/ceph
   sudo mkcephfs -a -c /etc/ceph/ceph.conf -k ceph.keyring
	
The script adds an admin key to the ``ceph.keyring``, which is analogous to a 
root password.
