================
KMIP Integration
================

`KMIP`_ can be used as a secure key management service for
:ref:`Server-Side Encryption <radosgw-encryption>` (SSE-KMS).

.. ditaa::

           +---------+       +---------+        +------+      +-------+
           |  Client |       | RadosGW |        | KMIP |      |  OSD  |
           +---------+       +---------+        +------+      +-------+
                | create secret   |                 |             |
                | key for key ID  |                 |             |
                |-----------------+---------------->|             |
                |                 |                 |             |
                | upload object   |                 |             |
                | with key ID     |                 |             |
                |---------------->| request secret  |             |
                |                 | key for key ID  |             |
                |                 |---------------->|             |
                |                 |<----------------|             |
                |                 | return secret   |             |
                |                 | key             |             |
                |                 |                 |             |
                |                 | encrypt object  |             |
                |                 | with secret key |             |
                |                 |--------------+  |             |
                |                 |              |  |             |
                |                 |<-------------+  |             |
                |                 |                 |             |
                |                 | store encrypted |             |
                |                 | object          |             |
                |                 |------------------------------>|

.. contents:: :depth: 2

Before you can use KMIP with Ceph, you will need to do three things.
You will need to associate Ceph with client information in KMIP,
and configure Ceph to use that client information.
You will also need to create one or more keys in KMIP.

Setting KMIP Access for Ceph
============================

Setting up Ceph in KMIP is very dependent on the mechanism(s) supported
by your implementation of KMIP.  Two implementations are described
here,

1. `IBM Security Guardium Key Lifecycle Manager (SKLM)`__.  This is a well
   supported commercial product.

2. `PyKMIP`_.  This is a small Python project, suitable for experimental
   and testing use only.

__ SKLM_

Using IBM SKLM
--------------

IBM `SKLM`_ supports client authentication using certificates.
Certificates may either be self-signed certificates created,
for instance, using OpenSSL, or certificates may be created
using SKLM.  Ceph should then be configured (see below) to
use KMIP and an attempt made to use it.  This will fail,
but it will leave an "untrusted client device certificate" in SKLM.
This can be then upgraded to a registered client using the web
interface to complete the registration process.

Find untrusted clients under ``Advanced Configuration``,
``Client Device Communication Certificates``.  Select
``Modify SSL/KMIP Certificates for Clients``, then toggle the flag
``allow the server to trust this certificate and communicate...``.

Using PyKMIP 
------------

`PyKMIP`_ has no special registration process, it simply
trusts the certificate.  However, the certificate has to
be issued by a certificate authority that is trusted by
PyKMIP.  PyKMIP also prefers that the certificate contain
an extension for "extended key usage".  However, that
can be defeated by specifying ``enable_tls_client_auth=False``
in the server configuration.

Creating Keys in KMIP
=====================

Some KMIP implementations come with a web interface or other
administrative tools to create and manage keys.  Refer to your
documentation on that if you wish to use it.  The KMIP protocol can also
be used to create and manage keys.  PyKMIP comes with a Python client
library that can be used this way.

In preparation to using the `pykmip` client, you'll need to have a valid
KMIP client key & certificate, such as the one you created for Ceph.

Next, you'll then need to download and install it:

.. prompt:: bash $

   virtualenv $HOME/my-kmip-env
   source $HOME/my-kmip-env/bin/activate
   pip install pykmip

Then you'll need to prepare a configuration file
for the client, something like this::

   cat <<EOF >$HOME/my-kmip-configuration
   [client]
   host={hostname}
   port=5696
   certfile={clientcert}
   keyfile={clientkey}
   ca_certs={clientca}
   ssl_version=PROTOCOL_TLSv1_2
   EOF

You will need to replace ``{hostname}`` with the name of your KMIP host,
also replace ``{clientcert}``, ``{clientkey}`` and ``{clientca}`` with pathnames to
a suitable PEM-encoded certificate, such as the one you created for
Ceph to use.

Now, you can run this Python script directly from
the shell::

  python
  from kmip.pie import client
  from kmip import enums
  import ssl
  import os
  import sys
  import json
  c = client.ProxyKmipClient(config_file=os.environ['HOME']+"/my-kmip-configuration")

  while True:
    l=sys.stdin.readline()
    keyname=l.strip()
    if keyname == "": break
    with c:
      key_id = c.create(
	  enums.CryptographicAlgorithm.AES,
	  256,
	  operation_policy_name='default',
	  name=keyname,
	  cryptographic_usage_mask=[
	      enums.CryptographicUsageMask.ENCRYPT,
	      enums.CryptographicUsageMask.DECRYPT
	  ]
      )
      c.activate(key_id)
      attrs = c.get_attributes(uid=key_id)
      r = {}
      for a in attrs[1]:
       r[str(a.attribute_name)] = str(a.attribute_value)
      print (json.dumps(r))

If this is all entered at the shell prompt, python will
prompt with ">>>" then "..." until the script is read in,
after which it will read and process names with no prompt
until a blank line or end of file (^D) is given it, or
an error occurs.  Of course you can turn this into a regular
Python script if you prefer.

Configure the Ceph Object Gateway
=================================

Edit the :ref:`Ceph configuration file <configuring-ceph>` to enable KMIP
as a KMS backend for server-side encryption::

  rgw crypt s3 kms backend = kmip
  rgw crypt kmip ca path = /etc/ceph/kmiproot.crt
  rgw crypt kmip client cert = /etc/ceph/kmip-client.crt
  rgw crypt kmip client key = /etc/ceph/private/kmip-client.key
  rgw crypt kmip kms key template = pykmip-$keyid

You may need to change the paths above to match where
you actually want to store KMIP certificate data.

The KMIP key template describes how Ceph will modify
the name given to it before it looks it up
in KMIP.  The default is just "$keyid".
If you don't want Ceph to see all your KMIP
keys, you can use this to limit Ceph to just the
designated subset of your KMIP key namespace.

Upload Object
=============

When uploading an object to the Ceph Object Gateway, provide the SSE key ID in the request.
As an example, using the AWS command-line client:

.. prompt:: bash $

   aws --endpoint=http://radosgw:8000 s3 cp plaintext.txt \
   s3://mybucket/encrypted.txt --sse=aws:kms --sse-kms-key-id mybucketkey
  
The Ceph Object Gateway will fetch the key from KMIP, encrypt the object and store
it in the bucket. Any request to download the object will make the Ceph Object Gateway
automatically retrieve the corresponding key from KMIP and decrypt the object.

Note that the secret will be fetched from KMIP using a name constructed
from the key template, replacing ``$keyid`` with the key provided.

With the Ceph configuration given above,
radosgw would fetch the secret from::

  pykmip-mybucketkey

.. _KMIP: http://www.oasis-open.org/committees/kmip/
.. _SKLM: https://www.ibm.com/products/ibm-security-key-lifecycle-manager
.. _PyKMIP: https://pykmip.readthedocs.io/en/latest/
