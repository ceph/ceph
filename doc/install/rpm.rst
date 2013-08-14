========================
 Installing RPM Packages
========================

You may install stable release packages (for stable deployments),
development release packages (for the latest features), or development
testing packages (for development and QA only).  Do not add multiple
package sources at the same time.

Install Release Key
===================

Packages are cryptographically signed with the ``release.asc`` key. Add our
release key to your system's list of trusted keys to avoid a security warning::

    sudo rpm --import 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc'


Install Prerequisites
=====================

Ceph may require additional additional third party libraries. 
To add the EPEL repository, execute the following:: 

   su -c 'rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'

Some releases of Ceph require the following packages:

- snappy
- leveldb
- gdisk
- python-argparse
- gperftools-libs

To install these packages, execute the following::  

	sudo yum install snappy leveldb gdisk python-argparse gperftools-libs


Add Release Packages
====================


Dumpling
--------

Dumpling is the most recent stable release of Ceph.  These packages are
recommended for anyone deploying Ceph in a production environment.
Critical bug fixes are backported and point releases are made as necessary.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 18 and 19
(``f18`` and ``f19``), OpenSUSE 12.2 (``opensuse12.2``), and SLES (``sles11``)
platforms. The repository package installs the repository details on your local
system for use with ``yum`` or ``up2date``.

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-dumpling/el6/noarch/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-dumpling


Cuttlefish
----------

Cuttlefish is the previous recent major release of Ceph.  These packages are
recommended for those who have already deployed bobtail in production and are
not yet ready to upgrade.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 17
(``f17``), OpenSUSE 12 (``opensuse12``), and SLES (``sles11``)
platforms. The repository package installs the repository details on
your local system for use with ``yum`` or ``up2date``.

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-cuttlefish/el6/noarch/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-cuttlefish


Bobtail
-------

Bobtail is the second major release of Ceph.  These packages are
recommended for those who have already deployed bobtail in production and
are not yet ready to upgrade.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 17
(``f17``), OpenSUSE 12 (``opensuse12``), and SLES (``sles11``)
platforms. The repository package installs the repository details on
your local system for use with ``yum`` or ``up2date``.

Replace the``{DISTRO}`` below with the distro codename::

    su -c 'rpm -Uvh http://ceph.com/rpm-bobtail/{DISTRO}/x86_64/ceph-release-1-0.el6.noarch.rpm'

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-bobtail/el6/x86_64/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-bobtail


Development Release Packages
----------------------------

Our development process generates a new release of Ceph every 3-4 weeks. These
packages are faster-moving than the stable releases. Development packages have
new features integrated quickly, while still undergoing several weeks of QA
prior to release.

Packages are cryptographically signed with the ``release.asc`` key. Add our
release key to your system's list of trusted keys to avoid a security warning::

    sudo rpm --import 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc'

Packages are currently built for the CentOS-6 and Fedora 17 platforms. The
repository package installs the repository details on your local system for use
with ``yum`` or ``up2date``.

For CentOS-6::

    su -c 'rpm -Uvh http://ceph.com/rpms/el6/x86_64/ceph-release-1-0.el6.noarch.rpm'

For Fedora 17:: 

    su -c 'rpm -Uvh http://ceph.com/rpms/fc17/x86_64/ceph-release-1-0.fc17.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-testing



Installing Ceph Deploy
======================

Once you have added either release or development packages to ``yum``, you
can install ``ceph-deploy``. ::

	sudo yum install ceph-deploy python-pushy



Installing Ceph Packages
========================

Once you have added either release or development packages to ``yum``, you
can install Ceph packages. You can also use ``ceph-deploy`` to install Ceph
packages. ::

	sudo yum install ceph



Installing Ceph Object Storage
==============================

:term:`Ceph Object Storage` runs on Apache and FastCGI in conjunction with the
:term:`Ceph Storage Cluster`. 

#. Install Apache and FastCGI. ::

	rpm -ivh fcgi-2.4.0-10.el6.x86_64.rpm 
 	rpm -ivh mod_fastcgi-2.4.6-2.el6.rf.x86_64.rpm


#. Install the Ceph Object Storage daemon. :: 

	yum install ceph-radosgw


#. Add the following lines to your Ceph configuration file.

.. code-block:: ini

  [client.radosgw.gateway]
        host = {fqdn}
        keyring = /etc/ceph/keyring.radosgw.gateway
        rgw socket path = /tmp/radosgw.sock
        log file = /var/log/ceph/radosgw.log
        rgw print continue = false
        
.. note:: Replace ``{fqdn}`` with the output from ``hostname``. This is 
   important. Debian systems use the simple hostname, but on CentOS 6/RHEL 6
   you must use the fully qualified domain name.
   
#. Create a data directory. :: 

	mkdir -p /var/lib/ceph/radosgw/ceph-radosgw.gateway


#. Change ``httpd ServerName`` in ``/etc/httpd/conf/httpd.conf``. ::

	ServerName {FQDN}
	
	
#. Create an Apache httpd virtual host in ``/etc/httpd/conf.d/rgw.conf``.

.. code-block:: ini

	FastCgiExternalServer /var/www/s3gw.fcgi -socket /tmp/radosgw.sock
	<VirtualHost *:80>
		ServerName <FQDN of the host>
		ServerAdmin root@localhost
		DocumentRoot /var/www
		RewriteEngine On
		RewriteRule ^/([a-zA-Z0-9-_.]*)([/]?.*) /s3gw.fcgi?page=$1&params=$2&%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]
		<IfModule mod_fastcgi.c>
			<Directory /var/www>
				Options +ExecCGI
				AllowOverride All
				SetHandler fastcgi-script
				Order allow,deny
				Allow from all
				AuthBasicAuthoritative Off
			</Directory>
		</IfModule>
		AllowEncodedSlashes On
		ErrorLog /var/log/httpd/error.log
		CustomLog /var/log/httpd/access.log combined
		ServerSignature Off
	</VirtualHost>

#. Turn off ``fastcgiwrapper`` in ``/etc/httpd/conf.d/fastcgi.conf`` by
   commenting out the following line:: 

	#FastCgiWrapper On


#. Add a ``fastcgi`` script. ::

	#!/bin/sh 
	exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.radosgw.gateway
	
	
#. Make ``s3gw.fcgi`` executable::
	
	chmod +x /var/www/rgw/s3gw.fcgi


#. Create a user key. ::

	ceph-authtool -C -n client.radosgw.gateway --gen-key /etc/ceph/keyring.radosgw.gateway
	ceph-authtool -n client.radosgw.gateway --cap mon 'allow rw' --cap osd 'allow rwx' /etc/ceph/keyring.radosgw.gateway
	ceph auth add client.radosgw.gateway --in-file=/etc/ceph/keyring.radosgw.gateway
	
	
#. Please make sure ``/etc/ceph/keyring.radosgw.gateway`` file and 
   ``/var/log/ceph/radosgw.log`` are accessible by the ``apache`` user. ::

	sudo chown apache:apache /etc/ceph/keyring.radosgw.gateway 
	sudo chown apache:apache /var/log/ceph/radosgw.log

.. note:: This is important. The user is ``root`` for Debian.


#. Create ``.rgw.buckets`` and add it to the Ceph Object Storage daemon. ::

     rados mkpool .rgw.buckets
     radosgw-admin pool add --pool .rgw.buckets	

#. Configure Apache and the Ceph Object Storage daemon to start on boot. :: 

	chkconfig httpd on
	chkconfig ceph-radosgw on

#. Start the services. ::

	/etc/init.d/httpd start
	/etc/init.d/ceph-radosgw start
	
See `Ceph Object Storage`_ for additional details.

.. _Ceph Object Storage: ../../radosgw
