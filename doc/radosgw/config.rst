=================================
 Configuring Ceph Object Gateway
=================================

Configuring a Ceph Object Gateway requires a running Ceph Storage Cluster, 
and an Apache web server with the FastCGI module.

The Ceph Object Gateway is a client of the Ceph Storage Cluster. As a 
Ceph Storage Cluster client, it requires:

- A name for the gateway instance. We use ``gateway`` in this guide.
- A storage cluster user name with appropriate permissions in a keyring.
- Pools to store its data.
- A data directory for the gateway instance.
- An instance entry in the Ceph Configuration file.
- A configuration file for the web server to interact with FastCGI.


Create a User and Keyring
=========================

Each instance must have a user name and key to communicate with a Ceph Storage
Cluster. In the following steps, we use an admin node to create a keyring. 
Then, we create a client user name and key. Next, we add the 
key to the Ceph Storage Cluster. Finally, we distribute the key ring to 
the node containing the gateway instance.

.. topic:: Monitor Key CAPS

   When you provide CAPS to the key, you MUST provide read capability.
   However, you have the option of providing write capability for the monitor. 
   This is an important choice. If you provide write capability to the key, 
   the Ceph Object Gateway will have the ability to create pools automatically; 
   however, it will create pools with either the default number of placement 
   groups (not ideal) or the number of placement groups you specified in your 
   Ceph configuration file. If you allow the Ceph Object Gateway to create 
   pools automatically, ensure that you have reasonable defaults for the number
   of placement groups first. See `Pool Configuration`_ for details.


See `User Management`_ for additional details on Ceph authentication.

#. Create a keyring for the gateway. ::

	sudo ceph-authtool --create-keyring /etc/ceph/ceph.client.radosgw.keyring
	sudo chmod +r /etc/ceph/ceph.client.radosgw.keyring
	

#. Generate a Ceph Object Gateway user name and key for each instance. For
   exemplary purposes, we will use the name ``gateway`` after ``client.radosgw``:: 

	sudo ceph-authtool /etc/ceph/ceph.client.radosgw.keyring -n client.radosgw.gateway --gen-key


#. Add capabilities to the key. See `Configuration Reference - Pools`_ for details
   on the effect of write permissions for the monitor and creating pools. ::

	sudo ceph-authtool -n client.radosgw.gateway --cap osd 'allow rwx' --cap mon 'allow rwx' /etc/ceph/ceph.client.radosgw.keyring


#. Once you have created a keyring and key to enable the Ceph Object Gateway 
   with access to the Ceph Storage Cluster, add the key to your 
   Ceph Storage Cluster. For example::

	sudo ceph -k /etc/ceph/ceph.client.admin.keyring auth add client.radosgw.gateway -i /etc/ceph/ceph.client.radosgw.keyring


#. Distribute the keyring to the node with the gateway instance. ::

	sudo scp /etc/ceph/ceph.client.radosgw.keyring  ceph@{hostname}:/home/ceph
	ssh {hostname}
	sudo mv ceph.client.radosgw.keyring /etc/ceph/ceph.client.radosgw.keyring



Create Pools
============

Ceph Object Gateways require Ceph Storage Cluster pools to store specific
gateway data.  If the user you created has permissions, the gateway
will create the pools automatically. However, you should ensure that you have
set an appropriate default number of placement groups per pool into your Ceph
configuration file.

.. note:: Ceph Object Gateways have multiple pools, so don't make the number of
   PGs too high considering all of the pools assigned to the same CRUSH 
   hierarchy, or performance may suffer.

When configuring a gateway with the default region and zone, the naming
convention for pools typically omits region and zone naming, but you can use any
naming convention you prefer. For example:


- ``.rgw.root``
- ``.rgw.control``
- ``.rgw.gc``
- ``.rgw.buckets``
- ``.rgw.buckets.index``
- ``.rgw.buckets.extra``
- ``.log``
- ``.intent-log``
- ``.usage``
- ``.users``
- ``.users.email``
- ``.users.swift``
- ``.users.uid``


See `Configuration Reference - Pools`_ for details on the default pools for
gateways. See `Pools`_ for details on creating pools. Execute the following 
to create a pool:: 

	ceph osd pool create {poolname} {pg-num} {pgp-num} {replicated | erasure} [{erasure-code-profile}]  {ruleset-name} {ruleset-number}


.. tip:: Ceph supports multiple CRUSH hierarchies and CRUSH rulesets, enabling 
   great flexibility in the way you configure your gateway. Pools such as 
   ``rgw.buckets.index`` may benefit from a pool of SSDs for fast performance. 
   Backing storage may benefit from the increased economy of erasure-coded 
   storage, and/or the improved performance from cache tiering.

When you have completed this step, execute the following to ensure that
you have created all of the foregoing pools::

	rados lspools


Add a Gateway Configuration to Ceph
===================================

Add the Ceph Object Gateway configuration to your Ceph Configuration file. The
Ceph Object Gateway configuration requires you to identify the Ceph Object
Gateway instance. Then, you must specify the host name where you installed the
Ceph Object Gateway daemon, a keyring (for use with cephx), the socket path for 
FastCGI and a log file. For example::  

	[client.radosgw.{instance-name}]
	host = {host-name}
	keyring = /etc/ceph/ceph.client.radosgw.keyring
	rgw socket path = /var/run/ceph/ceph.radosgw.{instance-name}.fastcgi.sock
	log file = /var/log/radosgw/client.radosgw.{instance-name}.log

The ``[client.radosgw.*]`` portion of the gateway instance identifies this
portion of the Ceph configuration file as configuring a Ceph Storage Cluster
client where the client type is  a Ceph Object Gateway (i.e., ``radosgw``). The
instance name follows. For example:: 

	[client.radosgw.gateway]
	host = ceph-gateway
	keyring = /etc/ceph/ceph.client.radosgw.keyring
	rgw socket path = /var/run/ceph/ceph.radosgw.gateway.fastcgi.sock
	log file = /var/log/radosgw/client.radosgw.gateway.log

.. note:: The ``host`` must be your machine hostname, not the FQDN. Make sure 
   that the name you use for the FastCGI socket is not the same as the one 
   used for the object gateway, which is 
   ``ceph-client.radosgw.{instance-name}.asok`` by default. You must use the 
   same name in your S3 FastCGI file too. See `Add a Ceph Object Gateway 
   Script`_ for details.

Configuring Print Continue
--------------------------

On CentOS/RHEL distributions, turn off ``print continue``. If you have it set
to ``true``, you may encounter problems with ``PUT`` operations. ::

	rgw print continue = false

Configuring Operations Logging
------------------------------

In early releases of Ceph (v0.66 and earlier), the Ceph Object Gateway will log
every successful operation in the Ceph Object Gateway backend by default. This
means that every request, whether it is a read request or a write request will
generate a gateway operation that writes data. This does not come without cost,
and may affect overall performance. Turning off logging completely can be done
by adding the following config option to the Ceph configuration file::

        rgw enable ops log = false

Another way to reduce the logging load is to send operations logging data to a
UNIX domain socket, instead of writing it to the Ceph Object Gateway backend::

        rgw ops log rados = false
        rgw enable ops log = true
        rgw ops log socket path = <path to socket>

When specifying a UNIX domain socket, it is also possible to specify the maximum
amount of memory that will be used to keep the data backlog::

        rgw ops log data backlog = <size in bytes>

Any backlogged data in excess to the specified size will be lost, so the socket
needs to be read constantly.


Enabling Subdomain S3 Calls
---------------------------

To use a Ceph Object Gateway with subdomain S3 calls (e.g.,
``http://bucketname.hostname``), you must add the Ceph Object Gateway DNS name
under the ``[client.radosgw.gateway]`` section of your Ceph configuration file::

	[client.radosgw.gateway]
		...
		rgw dns name = {hostname}

You should also consider installing a DNS server such as `Dnsmasq`_ on your
client machine(s) when using ``http://{bucketname}.{hostname}`` syntax. The
``dnsmasq.conf`` file should include the following settings:: 

	address=/{hostname}/{host-ip-address}
	listen-address={client-loopback-ip}

Then, add the ``{client-loopback-ip}`` IP address as the first DNS nameserver
on client the machine(s).

See `Add Wildcard to DNS`_ for details.


Redeploy Ceph Configuration
---------------------------

To use ``ceph-deploy`` to push a new copy of the configuration file to the hosts
in your cluster, execute the following::

	ceph-deploy config push {host-name [host-name]...}


Add a Ceph Object Gateway Script
================================

Add a ``s3gw.fcgi`` file (use the same name referenced in the first line 
of ``rgw.conf``). For Debian/Ubuntu distributions, save the file to the 
``/var/www`` directory. For CentOS/RHEL distributions, save the file to the
``/var/www/html`` directory. Assuming a cluster named ``ceph`` (default), 
and the user created in previous steps, the contents of the file should 
include::

	#!/bin/sh
	exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.radosgw.gateway

Ensure that you apply execute permissions to ``s3gw.fcgi``. ::

	sudo chmod +x s3gw.fcgi

On some distributions, you must also change the ownership to ``apache``. :: 

	sudo chown apache:apache s3gw.fcgi



Create Data Directory
=====================

Deployment scripts may not create the default Ceph Object Gateway data
directory.  Create data directories for each instance of a ``radosgw`` daemon
(if you haven't done so already). The ``host``  variables in the Ceph
configuration file determine which host runs each instance of a ``radosgw``
daemon. The typical form specifies the ``radosgw`` daemon, the cluster name and
the daemon ID. ::

	sudo mkdir -p /var/lib/ceph/radosgw/{$cluster}-{$id}

Using the exemplary ``ceph.conf`` settings above, you would execute the following::

	sudo mkdir -p /var/lib/ceph/radosgw/ceph-radosgw.gateway



Create a Gateway Configuration
==============================

On the host where you installed the Ceph Object Gateway, create an ``rgw.conf``
file. For Debian/Ubuntu systems, place the file in the
``/etc/apache2/sites-available`` directory. For CentOS/RHEL systems, place the
file in the ``/etc/httpd/conf.d`` directory. 

We recommend deploying FastCGI as an external server, because allowing Apache to
manage FastCGI sometimes introduces high latency. To manage FastCGI as an
external server, use the ``FastCgiExternalServer`` directive.  See
`FastCgiExternalServer`_ for details on this directive.  See `Module
mod_fastcgi`_ for general details. See `Apache Virtual Host documentation`_ for
details on ``<VirtualHost>`` format  and settings. See `<IfModule> Directive`_
for additional details. 

Ceph Object Gateway requires a rewrite rule for the Amazon S3-compatible
interface. It's required for passing in the ``HTTP_AUTHORIZATION env`` for S3,
which is filtered out by Apache. The rewrite rule is not necessary for the
OpenStack Swift-compatible interface.

You should configure Apache to allow encoded slashes, provide paths for log
files and to turn off server signatures. See below for an exemplary embodiment 
of a gateway configuration for Debian/Ubuntu and CentOS/RHEL.

.. rubric:: Debian/Ubuntu

.. literalinclude:: rgw-debian.conf
   :language: ini

.. rubric:: CentOS/RHEL

.. literalinclude:: rgw-centos.conf
   :language: ini


#. Replace the ``/{path}/{socket-name}`` entry with path to the socket and
   the socket name. For example, 
   ``/var/run/ceph/ceph.radosgw.gateway.fastcgi.sock``. Ensure that you use the 
   same path and socket name in your ``ceph.conf`` entry.

#. Replace the ``{fqdn}`` entry with the fully-qualified domain name of the 
   server. 
   
#. Replace the ``{email.address}`` entry with the email address for the 
   server administrator.
   
#. Add a ``ServerAlias`` if you wish to use S3-style subdomains 
   (of course you do).

#. Save the configuration to a file (e.g., ``rgw.conf``).

Finally, if you enabled SSL, make sure that you set the port to your SSL port
(usually 443) and your configuration file includes the following::

	SSLEngine on
	SSLCertificateFile /etc/apache2/ssl/apache.crt
	SSLCertificateKeyFile /etc/apache2/ssl/apache.key
	SetEnv SERVER_PORT_SECURE 443


.. _Module mod_fastcgi: http://www.fastcgi.com/drupal/node/25
.. _FastCgiExternalServer: http://www.fastcgi.com/drupal/node/25#FastCgiExternalServer
.. _Apache Virtual Host documentation: http://httpd.apache.org/docs/2.2/vhosts/
.. _<IfModule> Directive: http://httpd.apache.org/docs/2.2/mod/core.html#ifmodule
	
	
.. important:: If you are using CentOS, RHEL or a similar distribution, make 
   sure that ``FastCgiWrapper`` is turned ``off`` in 
   ``/etc/httpd/conf.d/fastcgi.conf``. It is usually ``on`` by default.

For Debian/Ubuntu distributions, enable the site for ``rgw.conf``. :: 

	sudo a2ensite rgw.conf

Then, disable the default site. :: 

	sudo a2dissite default
	

Adjust Path Ownership/Permissions
=================================

On some distributions, you must change ownership for ``/var/log/httpd`` or 
``/var/log/apache2`` and ``/var/run/ceph`` to ensure that Apache has permissions 
to create a socket or log file. ::

	sudo chown apache:apache /path/to/file   

On some systems, you may need to set SELinux to ``Permissive``. If you are
unable to communicate with the gateway after attempting to start it, try 
executing::

	getenforce
	
If the result is ``1`` or ``Enforcing``, execute::

	sudo setenforce 0

Then, restart Apache and the gateway daemon to see if that resolves the issue. 
If it does, you can configure your system to disable SELinux.


Restart Services and Start the Gateway
======================================

To ensure that all components have reloaded their configurations,  we recommend
restarting your ``ceph`` and ``apache`` services. Then,  start up the
``radosgw`` service.

For the Ceph Storage Cluster, see `Operating a Cluster`_ for details. Some 
versions of Ceph use different methods for starting and stopping clusters.


Restart Apache
--------------

On Debian/Ubuntu systems, use ``apache2``. For example::

	sudo service apache2 restart
	sudo /etc/init.d/apache2 restart

On CentOS/RHEL systems, use ``httpd``. For example:: 

	sudo /etc/init.d/httpd restart


Start the Gateway
-----------------

On Debian/Ubuntu systems, use ``radosgw``. For example:: 

	sudo /etc/init.d/radosgw start
	
On CentOS/RHEL systems, use ``ceph-radosgw``. For example::

	sudo /etc/init.d/ceph-radosgw start


Verify the Runtime
------------------

Once the service is up and running, you can make an anonymous GET request to see
if the gateway returns a response. A simple HTTP request to the domain name
should return the following:

.. code-block:: xml

   <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	   <Owner>
		   <ID>anonymous</ID>
		   <DisplayName/>
	   </Owner>
	   <Buckets/>
   </ListAllMyBucketsResult>


If you receive an error, check your settings and try again. See 
`Adjust Path Ownership/Permissions`_ for details.

Using The Gateway
=================

To use the REST interfaces, first create an initial Ceph Object Gateway user for
the S3 interface. Then, create a subuser for the swift interface. See the `Admin
Guide`_ for details.



.. _Dnsmasq: https://help.ubuntu.com/community/Dnsmasq
.. _Configuration Reference - Pools: ../config-ref#pools
.. _Pool Configuration: ../../rados/configuration/pool-pg-config-ref/
.. _Pools: ../../rados/operations/pools
.. _User Management: ../../rados/operations/user-management
.. _Operating a Cluster: ../../rados/rados/operations/operating
.. _Admin Guide: ../admin
.. _Add Wildcard to DNS: ../../install/install-ceph-gateway#add-wildcard-to-dns
