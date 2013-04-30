=================
 Ceph Deployment
=================

The ``ceph-deploy`` tool is a way to deploy Ceph relying only upon SSH access to
the servers, ``sudo``, and some Python. It runs on your workstation, and does
not require servers, databases, or anything any other tools. If you set up and
tear down Ceph clusters a lot, and want minimal extra bureaucracy,
``ceph-deploy`` is an ideal tool. The ``ceph-deploy`` tool is not a generic
deployment system. It was designed exclusivly for Ceph users who want to get
Ceph up and running quickly with sensible initial configuration settings without
the overhead of installing Chef, Puppet or Juju. Users who want fine-control
over security settings, partitions or directory  locations should use a tool
such as Juju, Puppet, `Chef`_ or Crowbar. 


.. raw:: html

	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Ceph Deploy</h3>

With ``ceph-deploy``, you can install Ceph packages on remote hosts,  create a
cluster, add monitors, gather (or forget) keys, add metadata servers and OSDs,
configure admin hosts, and tear down the clusters. With a single tool, you can
develop scripts to  create, deploy and tear down clusters quickly and easily.


.. raw:: html

	<table cellpadding="10"><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td>

.. toctree:: 

	Transition from mkcephfs <ceph-deploy-transition>   
   Preflight Checklist <preflight-checklist>	
	Install Ceph <ceph-deploy-install>
	Create a Cluster <ceph-deploy-new>
	Add/Remove Monitor(s) <ceph-deploy-mon>
	
	

.. raw:: html

	</td><td>	
	
.. toctree::
	
	Key Management <ceph-deploy-keys>
	Add/Remove OSD(s) <ceph-deploy-osd>
	Add/Remove MDS(s) <ceph-deploy-mds>
	Purge Hosts <ceph-deploy-purge>
	Admin Tasks <ceph-deploy-admin>

.. raw:: html 

	</td></tr></tbody></table></td><td><h3>mkcephfs (deprecated)</h3>
	
The ``mkcephfs`` utility generates an ``fsid`` and keys for your cluster, as
defined by the Ceph configuration file. It does not create directories for you
and relies on use of the ``root`` password. As of Ceph v0.60, it is deprecated
in favor of ``ceph-deploy``.

.. toctree:: 

	mkcephfs (deprecated) <mkcephfs>
	
.. raw:: html

	</td></tr></tbody></table>


.. _Chef: http://wiki.ceph.com/02Guides/Deploying_Ceph_with_Chef