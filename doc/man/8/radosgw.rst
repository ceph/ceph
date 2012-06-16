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

.. option:: --rgw-socket-path=path

   Specify a unix domain socket path.


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
        ceph-authtool -n client.radosgw.gateway --cap mon 'allow r' --cap osd 'allow rwx' --cap mds 'allow' /etc/ceph/keyring.radosgw.gateway

And add the key to the auth entries::

        ceph auth add client.radosgw.gateway --in-file=keyring.radosgw.gateway

Now you can start Apache and the radosgw daemon::

        /etc/init.d/apache2 start
        /etc/init.d/radosgw start

Availability
============

**radosgw** is part of the Ceph distributed file system. Please refer
to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
