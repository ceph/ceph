=================
 Ceph Deployment
=================

You can deploy Ceph using many different deployment systems including Chef, Juju, 
Puppet, and Crowbar. If you are just experimenting, Ceph provides some minimal 
deployment tools that rely only on SSH and DNS to deploy Ceph. You need to set
up the SSH and DNS settings manually.


.. raw:: html

	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Ceph Deployment Scripts</h3>

We provide light-weight deployment scripts to help you evaluate Ceph. For
professional deployment, you should consider professional deployment systems
such as Juju, Puppet, Chef or Crowbar. 

.. toctree:: 

	Ceph Deploy <ceph-deploy>
	mkcephfs (deprecated) <mkcephfs>

.. raw:: html 

	</td><td><h3>OpsCode Chef</h3>

.. toctree:: 

	Installing Chef <install-chef>
	Deploying with Chef <chef>
	
.. raw:: html

	</td></tr></tbody></table>
