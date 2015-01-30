===============================
 radosgw -- rados REST gateway
===============================

.. program:: radosgw

Synopsis
========

| **radosgw**


Description
===========

**radosgw** is an HTTP REST gateway for the RADOS object store, a part
of the Ceph distributed storage system. It is implemented as a FastCGI
module using libfcgi, and can be used in conjunction with any FastCGI
capable web server.


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through
   ``ceph.conf``).

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

Currently it's the easiest to use the RADOS Gateway with Apache and mod_fastcgi::

        FastCgiExternalServer /var/www/s3gw.fcgi -socket /tmp/radosgw.sock

        <VirtualHost *:80>
          ServerName rgw.example1.com
          ServerAlias rgw
          ServerAdmin webmaster@example1.com
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
          ServerSignature Off
        </VirtualHost>

And the corresponding radosgw script (/var/www/s3gw.fcgi)::

        #!/bin/sh
        exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.radosgw.gateway

The radosgw daemon is a standalone process which needs a configuration
section in the ceph.conf The section name should start with
'client.radosgw.' as specified in /etc/init.d/radosgw::

        [client.radosgw.gateway]
            host = gateway
            keyring = /etc/ceph/keyring.radosgw.gateway
            rgw socket path = /tmp/radosgw.sock

You will also have to generate a key for the radosgw to use for
authentication with the cluster::

        ceph-authtool -C -n client.radosgw.gateway --gen-key /etc/ceph/keyring.radosgw.gateway
        ceph-authtool -n client.radosgw.gateway --cap mon 'allow rw' --cap osd 'allow rwx' /etc/ceph/keyring.radosgw.gateway

And add the key to the auth entries::

        ceph auth add client.radosgw.gateway --in-file=keyring.radosgw.gateway

Now you can start Apache and the radosgw daemon::

        /etc/init.d/apache2 start
        /etc/init.d/radosgw start

Usage Logging
=============

The **radosgw** maintains an asynchronous usage log. It accumulates
statistics about user operations and flushes it periodically. The
logs can be accessed and managed through **radosgw-admin**.

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

**radosgw** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer
to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
:doc:`radosgw-admin <radosgw-admin>`\(8)
