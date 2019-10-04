===========================
HashiCorp Vault Integration
===========================

HashiCorp `Vault`_ can be used as a secure key management service for
`Server-Side Encryption`_ (SSE-KMS).

#. `Vault authentication`_
#. `Create a key in Vault`_
#. `Configure the Ceph Object Gateway`_
#. `Upload object`_

Vault authentication
====================

Vault provides several authentication mechanisms. Currently, the Object Gateway
supports the `token authentication method`_ only.

When authenticating with Vault using the token method, save the token in a
plain-text file. The path to this file must be provided in the Gateway
configuration file (see below). For security reasons, ensure the file is
readable by the Object Gateway only.

Create a key in Vault
=====================

Generate and save a 256-bit key in Vault. Vault provides several Secret
Engines, which store, generate, and encrypt data. Currently, the only secret
engine supported is the `KV Secrets engine`_ version 2.

To create a key in the KV version 2 engine using Vault's command line client,
use the commands below::

  export VAULT_ADDR='http://vaultserver:8200'
  vault kv put secret/myproject/mybucketkey key=$(dd bs=32 count=1 if=/dev/urandom of=/dev/stdout 2>/dev/null | base64)

Output::

  ====== Metadata ======
  Key              Value
  ---              -----
  created_time     2019-08-29T17:01:09.095824999Z
  deletion_time    n/a
  destroyed        false
  version          1

  === Data ===
  Key    Value
  ---    -----
  key    Ak5dRyLQjwX/wb7vo6Fq1qjsfk1dh2CiSicX+gLAhwk=

The URL to the secret in Vault must be provided in the Gateway configuration
file (see below).

Configure the Ceph Object Gateway
=================================

Edit the Ceph configuration file to enable Vault as a KMS for server-side
encryption::

   rgw crypt s3 kms backend = vault
   rgw crypt vault auth = token
   rgw crypt vault addr = http://vaultserver:8200
   rgw crypt vault token file = /path/to/token.file

Upload object
=============

When uploading an object, provide the SSE key ID in the request. As an example,
using the AWS command-line client::

  aws --endpoint=http://radosgw:8000 s3 cp plaintext.txt s3://mybucket/encrypted.txt --sse=aws:kms --sse-kms-key-id /v1/secret/data/myproject/mybucketkey

The object gateway will fetch the key from Vault (using the token for
authentication), encrypt the object and store it in the bucket. Any request to
downlod the object will require the correct key ID for the Gateway to
successfully the decrypt it.

.. _Server-Side Encryption: ../encryption
.. _Vault: https://www.vaultproject.io/docs/
.. _token authentication method: https://www.vaultproject.io/docs/auth/token.html
.. _KV Secrets engine: https://www.vaultproject.io/docs/secrets/kv/
