:orphan:

===============================
 radosgw -- rados REST gateway
===============================

.. program:: radosgw

Synopsis
========

| **radosgw**


Description
===========

:program:`radosgw` is an HTTP REST gateway for the RADOS object store, a part
of the Ceph distributed storage system. It is implemented as a FastCGI
module using libfcgi, and can be used in conjunction with any FastCGI
capable web server.


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ``ceph.conf``).

.. option:: -i ID, --id ID

   Set the ID portion of name for radosgw

.. option:: -n TYPE.ID, --name TYPE.ID

   Set the rados user name for the gateway (eg. client.radosgw.gateway)

.. option:: --cluster NAME

   Set the cluster name (default: ceph)

.. option:: -d

   Run in foreground, log to stderr

.. option:: -f

   Run in foreground, log to usual location

.. option:: --rgw-socket-path=path

   Specify a unix domain socket path.

.. option:: --rgw-region=region

   The region where radosgw runs

.. option:: --rgw-zone=zone

   The zone where radosgw runs


Configuration
=============

Earlier RADOS Gateway had to be configured with ``Apache`` and ``mod_fastcgi``.
Now, ``mod_proxy_fcgi`` module is used instead of ``mod_fastcgi``.
``mod_proxy_fcgi`` works differently than a traditional FastCGI module. This
module requires the service of ``mod_proxy`` which provides support for the
FastCGI protocol. So, to be able to handle FastCGI protocol, both ``mod_proxy``
and ``mod_proxy_fcgi`` have to be present in the server. Unlike ``mod_fastcgi``,
``mod_proxy_fcgi`` cannot start the application process. Some platforms have
``fcgistarter`` for that purpose. However, external launching of application
or process management may be available in the FastCGI application framework
in use.

``Apache`` can be configured in a way that enables ``mod_proxy_fcgi`` to be used
with localhost tcp or through unix domain socket. ``mod_proxy_fcgi`` that doesn't
support unix domain socket such as the ones in Apache 2.2 and earlier versions of
Apache 2.4, needs to be configured for use with localhost tcp. Later versions of
Apache like Apache 2.4.9 or later support unix domain socket and as such they
allow for the configuration with unix domain socket instead of localhost tcp.

The following steps show the configuration in Ceph's configuration file i.e,
``/etc/ceph/ceph.conf`` and the gateway configuration file i.e,
``/etc/httpd/conf.d/rgw.conf`` (RPM-based distros) or
``/etc/apache2/conf-available/rgw.conf`` (Debian-based distros) with localhost
tcp and through unix domain socket:

#. For distros with Apache 2.2 and early versions of Apache 2.4 that use
   localhost TCP and do not support Unix Domain Socket, append the following
   contents to ``/etc/ceph/ceph.conf``::

	[client.radosgw.gateway]
	host = {hostname}
	keyring = /etc/ceph/ceph.client.radosgw.keyring
	rgw socket path = ""
	log file = /var/log/ceph/client.radosgw.gateway.log
	rgw frontends = fastcgi socket_port=9000 socket_host=0.0.0.0
	rgw print continue = false

#. Add the following content in the gateway configuration file:

   For Debian/Ubuntu add in ``/etc/apache2/conf-available/rgw.conf``::

		<VirtualHost *:80>
		ServerName localhost
		DocumentRoot /var/www/html

		ErrorLog /var/log/apache2/rgw_error.log
		CustomLog /var/log/apache2/rgw_access.log combined

		# LogLevel debug

		RewriteEngine On

		RewriteRule .* - [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

		SetEnv proxy-nokeepalive 1

		ProxyPass / fcgi://localhost:9000/

		</VirtualHost>

   For CentOS/RHEL add in ``/etc/httpd/conf.d/rgw.conf``::

		<VirtualHost *:80>
		ServerName localhost
		DocumentRoot /var/www/html

		ErrorLog /var/log/httpd/rgw_error.log
		CustomLog /var/log/httpd/rgw_access.log combined

		# LogLevel debug

		RewriteEngine On

		RewriteRule .* - [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

		SetEnv proxy-nokeepalive 1

		ProxyPass / fcgi://localhost:9000/

		</VirtualHost>

#. For distros with Apache 2.4.9 or later that support Unix Domain Socket,
   append the following configuration to ``/etc/ceph/ceph.conf``::

	[client.radosgw.gateway]
	host = {hostname}
	keyring = /etc/ceph/ceph.client.radosgw.keyring
	rgw socket path = /var/run/ceph/ceph.radosgw.gateway.fastcgi.sock
	log file = /var/log/ceph/client.radosgw.gateway.log
	rgw print continue = false

#. Add the following content in the gateway configuration file:

   For CentOS/RHEL add in ``/etc/httpd/conf.d/rgw.conf``::

		<VirtualHost *:80>
		ServerName localhost
		DocumentRoot /var/www/html

		ErrorLog /var/log/httpd/rgw_error.log
		CustomLog /var/log/httpd/rgw_access.log combined

		# LogLevel debug

		RewriteEngine On

		RewriteRule .* - [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

		SetEnv proxy-nokeepalive 1

		ProxyPass / unix:///var/run/ceph/ceph.radosgw.gateway.fastcgi.sock|fcgi://localhost:9000/

		</VirtualHost>

   The latest version of Ubuntu i.e, 14.04 ships with ``Apache 2.4.7`` that
   does not have Unix Domain Socket support in it and as such it has to be
   configured with localhost tcp. The Unix Domain Socket support is available in
   ``Apache 2.4.9`` and later versions. A bug has been filed to backport the UDS
   support to ``Apache 2.4.7`` for ``Ubuntu 14.04``.
   See: https://bugs.launchpad.net/ubuntu/+source/apache2/+bug/1411030

#. Generate a key for radosgw to use for authentication with the cluster. ::

	ceph-authtool -C -n client.radosgw.gateway --gen-key /etc/ceph/keyring.radosgw.gateway
	ceph-authtool -n client.radosgw.gateway --cap mon 'allow rw' --cap osd 'allow rwx' /etc/ceph/keyring.radosgw.gateway

#. Add the key to the auth entries. ::

	ceph auth add client.radosgw.gateway --in-file=keyring.radosgw.gateway

#. Start Apache and radosgw.

   Debian/Ubuntu::

		sudo /etc/init.d/apache2 start
		sudo /etc/init.d/radosgw start

   CentOS/RHEL::

		sudo apachectl start
		sudo /etc/init.d/ceph-radosgw start

Usage Logging
=============

:program:`radosgw` maintains an asynchronous usage log. It accumulates
statistics about user operations and flushes it periodically. The
logs can be accessed and managed through :program:`radosgw-admin`.

The information that is being logged contains total data transfer,
total operations, and total successful operations. The data is being
accounted in an hourly resolution under the bucket owner, unless the
operation was done on the service (e.g., when listing a bucket) in
which case it is accounted under the operating user.

Following is an example configuration::

        [client.radosgw.gateway]
            rgw enable usage log = true
            rgw usage log tick interval = 30
            rgw usage log flush threshold = 1024
            rgw usage max shards = 32
            rgw usage max user shards = 1


The total number of shards determines how many total objects hold the
usage log information. The per-user number of shards specify how many
objects hold usage information for a single user. The tick interval
configures the number of seconds between log flushes, and the flush
threshold specify how many entries can be kept before resorting to
synchronous flush.


Availability
============

:program:`radosgw` is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at http://ceph.com/docs for
more information.


See also
========

:doc:`ceph <ceph>`\(8)
:doc:`radosgw-admin <radosgw-admin>`\(8)
