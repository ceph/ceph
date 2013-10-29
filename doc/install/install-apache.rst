=============================
 Installing Apache & FastCGI
=============================

The :term:`Ceph Object Gateway` runs on Apache and FastCGI. The Ceph Object
Gateway also supports 100-continue. However, you must install Ceph builds
of Apache and FastCGI for 100-continue support.


Install w/out 100-Continue
==========================

You may use standard Apache and FastCGI packages for your Ceph Object
Gateways. However, they will not provide 100-continue support.

Debian Packages
---------------

To install Apache and FastCGI Debian packages, execute the following:: 

	sudo apt-get install apache2 libapache2-mod-fastcgi


RPM Packages
------------

To install Apache and FastCGI RPMs, execute the following::

	rpm -ivh fcgi-2.4.0-10.el6.x86_64.rpm 
	rpm -ivh mod_fastcgi-2.4.6-2.el6.rf.x86_64.rpm

Or::

	sudo yum install httpd mod_fastcgi


Install w/ 100-Continue
=======================

The Ceph community provides a slightly optimized version of the  ``apache2``
and ``fastcgi`` packages. The material difference is that  the Ceph packages are
optimized for the ``100-continue`` HTTP response,  where the server determines
if it will accept the request by first  evaluating the request header. See `RFC
2616, Section 8`_ for details  on ``100-continue``. You can find the most recent
builds of Apache and FastCGI packages modified for Ceph at `gitbuilder.ceph.com`_.


Debian Packages
---------------

#. Add a ``ceph-apache.list`` file to your APT sources. :: 

	echo deb http://gitbuilder.ceph.com/apache2-deb-$(lsb_release -sc)-x86_64-basic/ref/master $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-apache.list

#. Add a ``ceph-fastcgi.list`` file to your APT sources. :: 

	echo deb http://gitbuilder.ceph.com/libapache-mod-fastcgi-deb-$(lsb_release -sc)-x86_64-basic/ref/master $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-fastcgi.list

#. Update your repository and install Apache and FastCGI:: 

	sudo apt-get update && sudo apt-get install apache2 libapache2-mod-fastcgi


RPM Packages
------------

To install Apache with 100-continue, execute the following steps:

#. Ensure you have ``yum-plugin-priorities`` installed and enabled. See 
   `Installing YUM Priorities`_ for details.

#. Add a ``ceph-apache.repo`` file to ``/etc/yum.repos.d``. Replace 
   ``{distro}`` with the name of your distribution (e.g., ``centos6``, 
   ``rhel6``, etc.) ::

	[apache2-ceph-noarch]
	name=Apache noarch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/apache2-rpm-{distro}-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

	[apache2-ceph-source]
	name=Apache source packages for Ceph
	baseurl=http://gitbuilder.ceph.com/apache2-rpm-{distro}-x86_64-basic/ref/master
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc


#. Add a ``ceph-fastcgi.repo`` file to ``/etc/yum.repos.d``. Replace 
   ``{distro}`` with the name of your distribution (e.g., ``centos6``, 
   ``rhel6``, etc.) ::

	[fastcgi-ceph-basearch]
	name=FastCGI basearch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-centos6-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc
	
	[fastcgi-ceph-noarch]
	name=FastCGI noarch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-centos6-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

	[fastcgi-ceph-source]
	name=FastCGI source packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-centos6-x86_64-basic/ref/master
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc


#. Update your repo and install Apache and FastCGI. :: 

	sudo yum update && sudo yum install httpd mod_fastcgi


Modify the Configuration
========================

To complete the installation, ensure that you have the rewrite module
enabled and FastCGI enabled. The steps differ slightly based upon the 
type of package installation. 

Debian-based Packages
---------------------

#. Open the ``apache2.conf`` file. :: 

	sudo vim /etc/apache2/apache2.conf


#. Add a line for the ``ServerName`` in the Apache configuration file. 
   Provide the fully qualified domain name of the server machine 
   (e.g., ``hostname -f``). ::

	ServerName {fqdn}

#. Enable the URL rewrite modules for Apache and FastCGI. ::

	sudo a2enmod rewrite
	sudo a2enmod fastcgi


#. Restart Apache so that the foregoing changes take effect. ::

	sudo service apache2 restart


RPM-based Packages
------------------


#. Open the ``httpd.conf`` file. :: 

	sudo vim /etc/httpd/conf/httpd.conf

#. Uncomment ``#ServerName`` and add the name of your server. 
   Provide the fully qualified domain name of the server machine 
   (e.g., ``hostname -f``).:: 

	ServerName {fgdn}

#. Ensure that the Rewrite module is enabled. :: 

	#if not present, add:
	LoadModule rewrite_module modules/mod_rewrite.so	

#. Save the ``httpd.conf`` file.

#. Ensure that the FastCGI module is enabled. The installer should
   include an ``/etc/httpd/conf.d/fastcgi.conf`` file that loads the
   FastCGI module. :: 

	#if not present, add:
	LoadModule fastcgi_module modules/mod_fastcgi.so

#. Restart Apache so that the foregoing changes take effect.. :: 

	etc/init.d/httpd restart

.. _RFC 2616, Section 8: http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html
.. _gitbuilder.ceph.com: http://gitbuilder.ceph.com
.. _Installing YUM Priorities: ../yum-priorities