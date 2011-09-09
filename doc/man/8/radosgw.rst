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


Examples
========

An apache example configuration for using the RADOS gateway::

        <VirtualHost *:80>
          ServerName rgw.example1.com
          ServerAlias rgw
          ServerAdmin webmaster@example1.com
          DocumentRoot /var/www/web1/web/

          #turn engine on
          RewriteEngine On

          #following is important for RGW/rados
          RewriteRule ^/([a-zA-Z0-9-_.]*)([/]?.*) /s3gw.fcgi?page=$1&params=$2&%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

          <IfModule mod_fcgid.c>
            SuexecUserGroup web1 web1
            <Directory /var/www/web1/web/>
              Options +ExecCGI
              AllowOverride All
              SetHandler fcgid-script
              FCGIWrapper /var/www/fcgi-scripts/web1/radosgw .fcgi
              Order allow,deny
              Allow from all
              AuthBasicAuthoritative Off
            </Directory>
          </IfModule>

          AllowEncodedSlashes On

          # ErrorLog /var/log/apache2/error.log
          # CustomLog /var/log/apache2/access.log combined
          ServerSignature Off

        </VirtualHost>

And the corresponding radosgw script::

        #!/bin/sh
	exec /usr/bin/radosgw -c /etc/ceph.conf

By default radosgw will run as single threaded and its execution will
be controlled by the fastcgi process manager. An alternative way to
run it would be by specifying (along the lines of) the following in
the apache config::

        FastCgiExternalServer /var/www/web1/web/s3gw.fcgi -socket /tmp/.radosgw.sock

and specify a unix domain socket path (either by passing a command
line option, or through ceph.conf).


Availability
============

**radosgw** is part of the Ceph distributed file system. Please refer
to the Ceph wiki at http://ceph.newdream.net/wiki for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
