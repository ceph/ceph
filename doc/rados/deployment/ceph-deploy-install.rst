====================
 Package Management
====================

Install
=======

To install Ceph packages on your cluster hosts, open a command line on your
client machine and type the following::

	ceph-deploy install {hostname [hostname] ...}

Without additional arguments, ``ceph-deploy`` will install the most recent
major release of Ceph to the cluster host(s). To specify a particular package, 
you may select from the following:

- ``--release <code-name>`` 
- ``--testing`` 
- ``--dev <branch-or-tag>`` 

For example:: 

	ceph-deploy install --release cuttlefish hostname1
	ceph-deploy install --testing hostname2
	ceph-deploy install --dev wip-some-branch hostname{1,2,3,4,5}
	
For additional usage, execute:: 

	ceph-deploy install -h


Uninstall
=========

To uninstall Ceph packages from your cluster hosts, open a terminal on
your admin host and type the following:: 

	ceph-deploy uninstall {hostname [hostname] ...}

On a Debian or Ubuntu system, you may also::

	ceph-deploy purge {hostname [hostname] ...}

The tool will unininstall ``ceph`` packages from the specified hosts.  Purge
additionally removes configuration files.

