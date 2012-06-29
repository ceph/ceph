.. index:: RADOS Gateway, radosgw

=========================================
 Radosgw installation and administration
=========================================

RADOS Gateway (radosgw or rgw) provides a RESTful API to the object
store. It interfaces with a web server via FastCGI, and with RADOS via
libradospp.

Configuring Ceph for RADOS Gateway
----------------------------------

In order for a host to act as a RADOS gateway, you must add a
``[client.radosgw.<name>]`` section to your Ceph configuration file
(typically ``/etc/ceph/ceph.conf``):

::

  [client.radosgw.gateway]
    host = gateway
    rgw socket path = /tmp/radosgw.sock

``host`` is the name of the host running radosgw. ``keyring`` points
to the keyring file for Cephx authentication. ``rgw socket path`` is
the location of the UNIX socket which radosgw binds to.

If your Ceph cluster has Cephx authentication enabled (highly
recommended) you also need to add the following option to tell radosgw
where it finds its authentication key:

::

  [client.radosgw.gateway]
    keyring = /etc/ceph/keyring.radosgw.gateway


Creating authentication credentials
-----------------------------------

To allow radosgw to sucessfully authenticate with the Ceph cluster,
use the ``ceph-authtool`` command to create a key and set its
capabilities:

::

  ceph-authtool -C -n client.radosgw.gateway \
    --gen-key /etc/ceph/keyring.radosgw.gateway
  ceph-authtool -n client.radosgw.gateway \
    --cap mon 'allow r' --cap osd 'allow rwx' --cap mds 'allow' \
    /etc/ceph/keyring.radosgw.gateway

Finally, add this key to the authentication entries:

::

  ceph auth add client.radosgw.gateway \
    --in-file=/etc/ceph/keyring.radosgw.gateway


Configuring the web server for radosgw
--------------------------------------


The radosgw FastCGI wrapper
---------------------------

A wrapper script, customarily named ``radosgw.cgi`` needs to go into
your preferred location -- typically your web server root directory.

::

  #!/bin/sh
  exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.radosgw.gateway


The ``-c`` option may be omitted if your Ceph configuration file
resides in its default location ((``/etc/ceph/ceph.conf``)). The
``-n`` option identifies the ``client`` section in the configuration
file that radosgw should parse -- if omitted, this would default to
``client.admin``.

Configuring Apache for radosgw
------------------------------

The recommended way of deploying radosgw is with Apache and
``mod_fastcgi``. Ensure that both ``mod_fastcgi`` and ``mod_rewrite``
are enabled in your Apache configuration. Set the
``FastCGIExternalServer`` option to point to the radosgw FastCGI
wrapper.

::

  <IfModule mod_fastcgi.c>
    FastCgiExternalServer /var/www/radosgw.fcgi -socket /tmp/radosgw.sock
  </IfModule>


Then, create a virtual host configuration as follows:

::

  <VirtualHost *:80>
    ServerName radosgw.example.com
    ServerAlias rgw.example.com
    ServerAdmin webmaster@example.com
    DocumentRoot /var/www

    <IfModule mod_rewrite.c>
      RewriteEngine On
      RewriteRule  ^/(.*) /radosgw.fcgi?%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]
    </IfModule>

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


Starting the daemons
--------------------

For the gateway to become operational, start both the radosgw daemon
and your web server:

::

  service radosgw start
  service apache start


Creating users
--------------

In order to be able to use the RESTful API, create a user with the
``radosgw-admin`` utility:

::

  $ radosgw-admin user create --uid=johndoe --display-name="John Doe" --email=john@example.com
  { "user_id": "johndoe",
    "rados_uid": 0,
    "display_name": "John Doe",
    "email": "john@example.com",
    "suspended": 0,
    "subusers": [],
    "keys": [
      { "user": "johndoe",
        "access_key": "QFAMEDSJP5DEKJO0DDXY",
        "secret_key": "iaSFLDVvDdQt6lkNzHyW4fPLZugBAI1g17LO0+87"}],
    "swift_keys": []}

Note that creating a user also creates an ``access_key`` and
``secret_key`` entry for use with any S3 API-compatible client.


Enabling Swift access
---------------------

Allowing access to the object store with Swift (OpenStack Object
Storage) compatible clients requires an additional step, the creation
of a subuser and a Swift access key.

::

  # radosgw-admin subuser create --uid=johndoe --subuser=johndoe:swift --access=full
  { "user_id": "johndoe",
    "rados_uid": 0,
    "display_name": "John Doe",
    "email": "john@example.com",
    "suspended": 0,
    "subusers": [
      { "id": "johndoe:swift",
        "permissions": "full-control"}],
    "keys": [
      { "user": "johndoe",
        "access_key": "QFAMEDSJP5DEKJO0DDXY",
        "secret_key": "iaSFLDVvDdQt6lkNzHyW4fPLZugBAI1g17LO0+87"}],
    "swift_keys": []}

  # radosgw-admin key create --subuser=johndoe:swift --key-type=swift
  { "user_id": "johndoe",
    "rados_uid": 0,
    "display_name": "John Doe",
    "email": "john@example.com",
    "suspended": 0,
    "subusers": [
       { "id": "johndoe:swift",
         "permissions": "full-control"}],
    "keys": [
      { "user": "johndoe",
        "access_key": "QFAMEDSJP5DEKJO0DDXY",
        "secret_key": "iaSFLDVvDdQt6lkNzHyW4fPLZugBAI1g17LO0+87"}],
    "swift_keys": [
      { "user": "johndoe:swift",
        "secret_key": "E9T2rUZNu2gxUjcwUBO8n\/Ev4KX6\/GprEuH4qhu1"}]}

With this configuration, you are able to use any Swift client to
connect to and use radosgw. As an example, you might use the ``swift``
command-line client utility that ships with the OpenStack Object
Storage packages.

::

  $ swift -V 1.0 -A http://radosgw.example.com/auth \
    -U johndoe:swift -K E9T2rUZNu2gxUjcwUBO8n\/Ev4KX6\/GprEuH4qhu1 \
    post test
  $ swift -V 1.0 -A http://radosgw.example.com/auth \
    -U johndoe:swift -K E9T2rUZNu2gxUjcwUBO8n\/Ev4KX6\/GprEuH4qhu1 \
    upload test myfile

Note that the radosgw ``user:subuser`` tuple maps to the
``tenant:user`` tuple expected by Swift.

Note also that the radosgw Swift authentication service only supports
built-in Swift authentication (``-V 1.0``) at this point. There is
currently no way to make radosgw authenticate users via OpenStack
Identity Service (Keystone).
