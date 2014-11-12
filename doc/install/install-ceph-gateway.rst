=============================
 Install Ceph Object Gateway
=============================

The :term:`Ceph Object Gateway` daemon runs on Apache and FastCGI. 

To run a :term:`Ceph Object Storage` service, you must install Apache and
FastCGI. Then, you must install the Ceph Object Gateway daemon. The Ceph Object
Gateway supports 100-continue, but you must install Ceph builds of Apache and
FastCGI for 100-continue support. To install the Ceph Object Gateway, first
install and configure Apache and FastCGI. Then, install the Ceph Object Gateway
daemon. If you plan to run a Ceph Object Storage service with a federated
architecture (multiple regions and zones), you must also install the
synchronization agent.

See `Get Packages`_ for information on adding Ceph packages to each Ceph Node. 
Ensure that you have executed those steps on each Ceph Node first.


Apache/FastCGI w/out 100-Continue
=================================

You may use standard Apache and FastCGI packages for your Ceph Object
Gateways. However, they will not provide 100-continue support.

Debian Packages
---------------

To install Apache and FastCGI Debian packages, execute the following:: 

	sudo apt-get install apache2 libapache2-mod-fastcgi


RPM Packages
------------

To install Apache and FastCGI RPMs, execute the following::

	sudo rpm -ivh fcgi-2.4.0-10.el6.x86_64.rpm 
	sudo rpm -ivh mod_fastcgi-2.4.6-2.el6.rf.x86_64.rpm

Or::

	sudo yum install httpd mod_fastcgi


Apache/FastCGI w/ 100-Continue
==============================

The Ceph community provides a slightly optimized version of the  ``apache2``
and ``fastcgi`` packages. The material difference is that  the Ceph packages are
optimized for the ``100-continue`` HTTP response,  where the server determines
if it will accept the request by first  evaluating the request header. See `RFC
2616, Section 8`_ for details  on ``100-continue``. You can find the most recent
builds of Apache and FastCGI packages modified for Ceph at `gitbuilder.ceph.com`_.


Debian Packages
---------------

#. Add the development key::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/autobuild.asc | sudo apt-key add -

#. Add a ``ceph-apache.list`` file to your APT sources. :: 

	echo deb http://gitbuilder.ceph.com/apache2-deb-$(lsb_release -sc)-x86_64-basic/ref/master $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-apache.list

#. Add a ``ceph-fastcgi.list`` file to your APT sources. :: 

	echo deb http://gitbuilder.ceph.com/libapache-mod-fastcgi-deb-$(lsb_release -sc)-x86_64-basic/ref/master $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-fastcgi.list

#. Update your repository and install Apache and FastCGI:: 

	sudo apt-get update && sudo apt-get install apache2 libapache2-mod-fastcgi


RPM Packages
------------

To install Apache with 100-continue, execute the following steps:

#. Install ``yum-plugin-priorities``. ::

	sudo yum install yum-plugin-priorities

#. Ensure ``/etc/yum/pluginconf.d/priorities.conf`` exists.

#. Ensure ``priorities.conf`` enables the plugin. :: 

	[main]
	enabled = 1

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
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-{distro}-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc
	
	[fastcgi-ceph-noarch]
	name=FastCGI noarch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-{distro}-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

	[fastcgi-ceph-source]
	name=FastCGI source packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-{distro}-x86_64-basic/ref/master
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

   If the repository doesn't have a ``noarch`` section, you may remove the
   ``noarch`` entry above.


#. Update your repository. On RHEL systems, enable the 
   ``rhel-6-server-optional-rpms`` repository. ::

	sudo yum update --enablerepo=rhel-6-server-optional-rpms

#. Install Apache and FastCGI. :: 

	sudo yum update && sudo yum install httpd mod_fastcgi


Configure Apache/FastCGI
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

	ServerName {fqdn}

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

	sudo /etc/init.d/httpd restart



Enable SSL
==========

Some REST clients use HTTPS by default. So you should consider enabling SSL
for Apache. Use the following procedures to enable SSL.

.. note:: You can use self-certified certificates. Some client
   APIs check for a trusted certificate authority. You may need to obtain
   a SSL certificate from a trusted authority to use those client APIs.


Debian Packages
---------------

To enable SSL for Debian/Ubuntu systems, execute the following steps:

#. Ensure that you have installed the dependencies. :: 

	sudo apt-get install openssl ssl-cert

#. Enable the SSL module. ::

	sudo a2enmod ssl

#. Generate a certificate. ::

	sudo mkdir /etc/apache2/ssl
	sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/apache2/ssl/apache.key -out /etc/apache2/ssl/apache.crt

#. Restart Apache. ::

	sudo service apache2 restart


See the `Ubuntu Server Guide`_ for additional details.


RPM Packages
------------

To enable SSL for RPM-based systems, execute the following steps:

#. Ensure that you have installed the dependencies. ::

	sudo yum install mod_ssl openssl

#. Ensure the SSL module is enabled.

#. Generate a certificate and copy it to the appropriate locations. ::

	openssl x509 -req -days 365 -in ca.csr -signkey ca.key -out ca.crt
	cp ca.crt /etc/pki/tls/certs
	cp ca.key /etc/pki/tls/private/ca.key
	cp ca.csr /etc/pki/tls/private/ca.csr

#. Restart Apache. ::

	sudo /etc/init.d/httpd restart

See `Setting up an SSL secured Webserver with CentOS`_ for additional details.



Add Wildcard to DNS
===================

To use Ceph with S3-style subdomains (e.g., ``bucket-name.domain-name.com``),
you need to add a wildcard to the DNS record of the DNS server you use with the
``radosgw`` daemon.

.. tip:: The address of the DNS must also be specified in the Ceph 
   configuration file with the ``rgw dns name = {hostname}`` setting.

For ``dnsmasq``, consider addding the following ``address`` setting with a dot
(.) prepended to the host name:: 

	address=/.{hostname-or-fqdn}/{host-ip-address}
	address=/.ceph-node/192.168.0.1

For ``bind``, consider adding the a wildcard to the DNS record::

	$TTL	604800
	@	IN	SOA	ceph-node. root.ceph-node. (
				      2		; Serial
				 604800		; Refresh
				  86400		; Retry
				2419200		; Expire
				 604800 )	; Negative Cache TTL
	;
	@	IN	NS	ceph-node.
	@	IN	A	192.168.122.113
	*	IN	CNAME	@

Restart your DNS server and ping your server with a subdomain to 
ensure that your Ceph Object Store ``radosgw`` daemon can process
the subdomain requests. :: 

	ping mybucket.{fqdn}
	ping mybucket.ceph-node
	

Install Ceph Object Gateway
===========================

Ceph Object Storage services use the Ceph Object Gateway daemon (``radosgw``)
to enable the gateway. For federated architectures, the synchronization 
agent (``radosgw-agent``) provides data and metadata synchronization between
zones and regions. 


Debian Packages
---------------

To install the Ceph Object Gateway daemon, execute the following::

	sudo apt-get install radosgw
	

To install the Ceph Object Gateway synchronization agent, execute the
following::
	
	sudo apt-get install radosgw-agent


RPM Packages
------------

To install the Ceph Object Gateway daemon, execute the
following:: 

	sudo yum install ceph-radosgw ceph


To install the Ceph Object Gateway synchronization agent, execute the
following::

	sudo yum install radosgw-agent
	
	
Configure The Gateway
=====================

Once you have installed the Ceph Object Gateway packages, the next step is
to configure your Ceph Object Gateway. There are two approaches: 

- **Simple:** A `simple`_ Ceph Object Gateway configuration implies that you 
  are running a Ceph Object Storage service in a single data center. So you can
  configure the Ceph Object Gateway without regard to regions and zones.

- **Federated:** A `federated`_ Ceph Object Gateway configuration implies that
  you are running a Ceph Object Storage service in a geographically distributed 
  manner for fault tolerance and failover. This involves configuring your
  Ceph Object Gateway instances with regions and zones.

Choose the approach that best reflects your cluster.
	

.. _Get Packages: ../get-packages
.. _Ubuntu Server Guide: https://help.ubuntu.com/12.04/serverguide/httpd.html
.. _Setting up an SSL secured Webserver with CentOS: http://wiki.centos.org/HowTos/Https
.. _RFC 2616, Section 8: http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html
.. _gitbuilder.ceph.com: http://gitbuilder.ceph.com
.. _Installing YUM Priorities: ../yum-priorities
.. _simple: ../../radosgw/config
.. _federated: ../../radosgw/federated-config
