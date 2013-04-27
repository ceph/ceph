====================
 Package Management
====================

Install
=======

To install Ceph packages on your cluster hosts, open a command line on your
client machine and type the following::

	ceph-deploy install {hostname [hostname] ...}

Without additional arguments, ``ceph-deploy`` will install the most recent
stable Ceph package to the cluster host(s). To specify a particular package, 
you may select from the following:

- ``--stable <code-name>`` 
- ``--testing`` 
- ``--dev <branch-or-tag>`` 

For example:: 

	ceph-deploy install --stable <code-name> <hostname1 [hostname2] ...>
	ceph-deploy install --stable bobtail hostname{1,2,3,4,5}
	ceph-deploy install --testing hostname1
	ceph-deploy install --dev wip-some-branch hostname{1,2,3,4,5}
	
For additional details, see `Installing Debian/Ubuntu Packages`_.
For additional usage, execute:: 

	ceph-deploy install -h


Uninstall
=========

To uninstall Ceph packages from your cluster hosts, open a commandline on 
your admin host and type the following:: 

	ceph-deploy uninstall {hostname [hostname] ...}

The tool will unininstall ``ceph`` packages from the specified hosts. Other
commands may also invoke ``ceph-deploy uninstall``, such as ``purge``.

.. _Installing Debian/Ubuntu Packages:  ../../../install/debian


