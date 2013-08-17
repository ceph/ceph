=================
 Ceph Deployment
=================

The ``ceph-deploy`` tool is a way to deploy Ceph relying only upon SSH access to
the servers, ``sudo``, and some Python. It runs on your workstation, and does
not require servers, databases, or any other tools. If you set up and
tear down Ceph clusters a lot, and want minimal extra bureaucracy,
``ceph-deploy`` is an ideal tool. The ``ceph-deploy`` tool is not a generic
deployment system. It was designed exclusively for Ceph users who want to get
Ceph up and running quickly with sensible initial configuration settings without
the overhead of installing Chef, Puppet or Juju. Users who want fine-control
over security settings, partitions or directory  locations should use a tool
such as Juju, Puppet, `Chef`_ or Crowbar. 


With ``ceph-deploy``, you can develop scripts to install Ceph packages on remote
hosts, create a cluster, add monitors, gather (or forget) keys, add OSDs and
metadata servers, configure admin hosts, and tear down the clusters.

.. raw:: html

	<table cellpadding="10"><tbody valign="top"><tr><td>

.. toctree:: 

   Preflight Checklist <preflight-checklist>	
	Install Ceph <ceph-deploy-install>

.. raw:: html

	</td><td>	
	
.. toctree::
	
	Create a Cluster <ceph-deploy-new>
	Add/Remove Monitor(s) <ceph-deploy-mon>
	Key Management <ceph-deploy-keys>
	Add/Remove OSD(s) <ceph-deploy-osd>
	Add/Remove MDS(s) <ceph-deploy-mds>


.. raw:: html

	</td><td>	

.. toctree::

	Purge Hosts <ceph-deploy-purge>
	Admin Tasks <ceph-deploy-admin>

	
.. raw:: html

	</td></tr></tbody></table>


.. _Chef: http://wiki.ceph.com/02Guides/Deploying_Ceph_with_Chef