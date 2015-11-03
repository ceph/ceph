=============================
 Install Ceph Object Gateway
=============================

.. note:: To run the Ceph object gateway service, you should have a running
   Ceph cluster, the gateway host should have access to storage and public
   networks, and SELinux should be in permissive mode in rpm-based distros.

The :term:`Ceph Object Gateway` daemon runs on Apache and FastCGI.

To run a :term:`Ceph Object Storage` service, you must install Apache and
Ceph Object Gateway daemon on the host that is going to provide the gateway
service, i.e, the ``gateway host``. If you plan to run a Ceph Object Storage
service with a federated architecture (multiple regions and zones), you must
also install the synchronization agent.

.. note:: Previous versions of Ceph shipped with ``mod_fastcgi``. The current
   version ships with ``mod_proxy_fcgi`` instead.

In distros that ship Apache 2.4 (such as RHEL 7, CentOS 7 or Ubuntu 14.04
``Trusty``), ``mod_proxy_fcgi`` is already present. When you install the
``httpd`` package with ``yum`` or the ``apache2`` package with ``apt-get``,
``mod_proxy_fcgi`` becomes available for use on your server.

In distros that ship Apache 2.2 (such as RHEL 6, CentOS 6 or Ubuntu 12.04
``Precise``), ``mod_proxy_fcgi`` comes as a separate package. In
**RHEL 6/CentOS 6**, it is available in ``EPEL 6`` repo and can be installed with
``yum install mod_proxy_fcgi``. For **Ubuntu 12.04**, a backport for
``mod_proxy_fcgi`` is in progress and a bug has been filed for the same.
See: `ceph radosgw needs mod-proxy-fcgi for apache 2.2`_


Install Apache
==============

To install Apache on the ``gateway host``, execute the following:

On Debian-based distros, run::

	sudo apt-get install apache2

On RPM-based distros, run::

	sudo yum install httpd


Configure Apache
================

Make the following changes in Apache's configuration on the ``gateway host``:

Debian-based distros
--------------------

#. Add a line for the ``ServerName`` in ``/etc/apache2/apache2.conf``. Provide
   the fully qualified domain name of the server machine
   (e.g., ``hostname -f``)::

	ServerName {fqdn}

#. Load ``mod_proxy_fcgi`` module.

   Execute::

		sudo a2enmod proxy_fcgi

#. Start Apache service::

	sudo service apache2 start

RPM-based distros
-----------------

#. Open the ``httpd.conf`` file::

	sudo vim /etc/httpd/conf/httpd.conf

#. Uncomment ``#ServerName`` in the file and add the name of your server. Provide
   the fully qualified domain name of the server machine
   (e.g., ``hostname -f``)::

	ServerName {fqdn}

#. Update ``/etc/httpd/conf/httpd.conf`` to load ``mod_proxy_fcgi`` module. Add
   the following to the file::

	<IfModule !proxy_fcgi_module>
	LoadModule proxy_fcgi_module modules/mod_proxy_fcgi.so
	</IfModule>

#. Edit the line ``Listen 80`` in ``/etc/httpd/conf/httpd.conf`` with the public
   IP address of the host that you are configuring as a gateway server. Write
   ``Listen {IP ADDRESS}:80`` in place of ``Listen 80``.

#. Start httpd service

   Execute::

		sudo service httpd start

   Or::

		sudo systemctl start httpd


Enable SSL
==========

Some REST clients use HTTPS by default. So you should consider enabling SSL
for Apache. Use the following procedures to enable SSL.

.. note:: You can use self-certified certificates. Some client
   APIs check for a trusted certificate authority. You may need to obtain
   a SSL certificate from a trusted authority to use those client APIs.


Debian-based distros
--------------------

To enable SSL on Debian-based distros, execute the following steps:

#. Ensure that you have installed the dependencies::

	sudo apt-get install openssl ssl-cert

#. Enable the SSL module::

	sudo a2enmod ssl

#. Generate a certificate::

	sudo mkdir /etc/apache2/ssl
	sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/apache2/ssl/apache.key -out /etc/apache2/ssl/apache.crt

#. Restart Apache::

	sudo service apache2 restart


See the `Ubuntu Server Guide`_ for additional details.


RPM-based distros
-----------------

To enable SSL on RPM-based distros, execute the following steps:

#. Ensure that you have installed the dependencies::

	sudo yum install mod_ssl openssl

#. Generate private key::

	openssl genrsa -out ca.key 2048

#. Generate CSR::

	openssl req -new -key ca.key -out ca.csr

#. Generate a certificate::

	openssl x509 -req -days 365 -in ca.csr -signkey ca.key -out ca.crt

#. Copy the files to appropriate locations::

	sudo cp ca.crt /etc/pki/tls/certs
	sudo cp ca.key /etc/pki/tls/private/ca.key
	sudo cp ca.csr /etc/pki/tls/private/ca.csr

#. Update the Apache SSL configuration file ``/etc/httpd/conf.d/ssl.conf``.

   Give the correct location of ``SSLCertificateFile``::

		SSLCertificateFile /etc/pki/tls/certs/ca.crt

   Give the correct location of ``SSLCertificateKeyFile``::

		SSLCertificateKeyFile /etc/pki/tls/private/ca.key

   Save the changes.

#. Restart Apache.

   Execute::

		sudo service httpd restart

   Or::

		sudo systemctl restart httpd

See `Setting up an SSL secured Webserver with CentOS`_ for additional details.


Install Ceph Object Gateway Daemon
==================================

Ceph Object Storage services use the Ceph Object Gateway daemon (``radosgw``)
to enable the gateway. For federated architectures, the synchronization 
agent (``radosgw-agent``) provides data and metadata synchronization between
zones and regions. 


Debian-based distros
--------------------

To install the Ceph Object Gateway daemon on the `gateway host`, execute the
following::

	sudo apt-get install radosgw
	

To install the Ceph Object Gateway synchronization agent, execute the
following::
	
	sudo apt-get install radosgw-agent


RPM-based distros
-----------------

To install the Ceph Object Gateway daemon on the ``gateway host``, execute the
following:: 

	sudo yum install ceph-radosgw


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

.. _ceph radosgw needs mod-proxy-fcgi for apache 2.2: https://bugs.launchpad.net/precise-backports/+bug/1422417
.. _Ubuntu Server Guide: https://help.ubuntu.com/12.04/serverguide/httpd.html
.. _Setting up an SSL secured Webserver with CentOS: http://wiki.centos.org/HowTos/Https
.. _simple: ../../radosgw/config
.. _federated: ../../radosgw/federated-config
