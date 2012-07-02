===========================
 Configuring RADOS Gateway
===========================

Before you can start RADOS Gateway, you must modify your ``ceph.conf`` file
to include a section for RADOS Gateway You must also create an ``rgw.conf`` 
file in the ``/etc/apache2/sites-enabled`` directory. The ``rgw.conf`` 
file configures Apache to interact with FastCGI.

Add a RADOS GW Configuration to ``ceph.conf``
---------------------------------------------

Add the RADOS Gateway configuration to your ``ceph.conf`` file.  The RADOS
Gateway configuration requires you to specify the host name where you installed
RADOS Gateway, a keyring (for use with cephx), the socket path and a log file. 
For example::  

	[client.radosgw.gateway]
		host = {host-name}
		keyring = /etc/ceph/keyring.rados.gateway
		rgw socket path = /tmp/radosgw.sock
		log file = /var/log/ceph/radosgw.log

If you deploy Ceph with ``mkcephfs``, manually redeploy ``ceph.conf`` to the 
hosts in your cluster. For example:: 

	cd /etc/ceph
	ssh {host-name} sudo /etc/ceph/ceph.conf < ceph.conf

Create ``rgw.conf``
-------------------

Create an ``rgw.conf`` file on the host where you installed RADOS Gateway
under the ``/etc/apache2/sites-enabled`` directory.


We recommend deploying FastCGI as an external server, because allowing
Apache to manage FastCGI sometimes introduces high latency. To manage FastCGI 
as an external server, use the ``FastCgiExternalServer`` directive. 
See `FastCgiExternalServer`_ for details on this directive. 
See `Module mod_fastcgi`_ for general details. :: 

	FastCgiExternalServer /var/www/s3gw.fcgi -socket /tmp/radosgw.sock

.. _Module mod_fastcgi: http://www.fastcgi.com/drupal/node/25
.. _FastCgiExternalServer: http://www.fastcgi.com/drupal/node/25#FastCgiExternalServer

Once you have configured FastCGI as an external server, you must 
create the virtual host configuration within your ``rgw.conf`` file. See 
`Apache Virtual Host documentation`_ for details on ``<VirtualHost>`` format 
and settings. Replace the values in brackets. ::

	<VirtualHost *:80>
		ServerName {fqdn}
		ServerAdmin {email.address}
		DocumentRoot /var/www
	</VirtualHost>

.. _Apache Virtual Host documentation: http://httpd.apache.org/docs/2.2/vhosts/

RADOS Gateway requires a rewrite rule for the Amazon S3-compatible interface. 
It's required for passing in the ``HTTP_AUTHORIZATION env`` for S3, which is 
filtered out by Apache. The rewrite rule is not necessary for the OpenStack 
Swift-compatible interface. Turn on the rewrite engine and add the following
rewrite rule to your Virtual Host configuration. :: 

	RewriteEngine On
	RewriteRule ^/([a-zA-Z0-9-_.]*)([/]?.*) /s3gw.fcgi?page=$1&params=$2&%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]
	
Since the ``<VirtualHost>`` is running ``mod_fastcgi.c``, you must include a
section in your ``<VirtualHost>`` configuration for the ``mod_fastcgi.c`` module. 

::

	<VirtualHost *:80>
		...
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
		...
	</VirtualHost>
	
See `<IfModule> Directive`_ for additional details. 

.. _<IfModule> Directive: http://httpd.apache.org/docs/2.2/mod/core.html#ifmodule
	
Finally, you should configure Apache to allow encoded slashes, provide paths for
log files and to trun off server signatures. :: 	

	<VirtualHost *:80>	
	...	
		AllowEncodedSlashes On
		ErrorLog /var/log/apache2/error.log
		CustomLog /var/log/apache2/access.log combined
		ServerSignature Off
	</VirtualHost>
	

Enable the RADOS Gateway Configuration
--------------------------------------

Enable the site for ``rgw.conf``. :: 

	sudo a2ensite rgw.conf

Disable the default site. :: 

	sudo a2dissite default
	

Add a RADOS GW Script
---------------------

Add a ``s3gw.fcgi`` file (use the same name referenced in the first line 
of ``rgw.conf``) to ``/var/www``. The contents of the file should include:: 

	#!/bin/sh
	exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.rados.gateway
	
Ensure that you apply execute permissions to ``s3gw.fcgi``. ::

	sudo chmod +x s3gw.fcgi


Generate a Keyring and Key for RADOS Gateway
--------------------------------------------

You must create a keyring for the RADOS Gateway. For example:: 

	sudo ceph-authtool --create-keyring /etc/ceph/keyring.rados.gateway
	sudo chmod +r /etc/ceph/keyring.rados.gateway
	
Generate a key so that RADOS Gateway can identify a user name and authenticate 
the user with the cluster. Then, add capabilities to the key. For example:: 

	sudo ceph-authtool /etc/ceph/keyring.rados.gateway -n client.rados.gateway --gen-key
	sudo ceph-authtool -n client.rados.gateway --cap osd 'allow rwx' --cap mon 'allow r' /etc/ceph/keyring.rados.gateway
	
Add to Ceph Keyring Entries 
---------------------------
Once you have created a keyring and key for RADOS GW, add it as an entry in
the Ceph keyring. For example::

	ceph -k /etc/ceph/ceph.keyring auth add client.rados.gateway -i /etc/ceph/keyring.rados.gateway
	
Restart Services and Start the RADOS Gateway
--------------------------------------------
To ensure that all components have reloaded their configurations, 
we recommend restarting your ``ceph`` and ``apaches`` services. Then, 
start up the ``radosgw`` service. For example:: 

	sudo service ceph restart
	sudo service apache2 restart
	sudo service radosgw start

Create a RADOS Gateway User
---------------------------
To use the REST interfaces, first create an initial RADOS Gateway user. 
The RADOS Gateway user is not the same user as the ``client.rados.gateway``
user, which identifies the RADOS Gateway as a user of the RADOS cluster.
The RADOS Gateway user is a user of the RADOS Gateway.

For example:: 

	sudo radosgw-admin user create --uid="{username}" --displayname="{Display Name}"

For details on RADOS Gateway administration, see `radosgw-admin`_. 

.. _radosgw-admin: ../../man/8/radosgw-admin/ 

