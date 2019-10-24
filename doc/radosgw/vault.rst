===========================
HashiCorp Vault Integration
===========================

HashiCorp `Vault`_ can be used as a secure key management service for
`Server-Side Encryption`_ (SSE-KMS).

#. `Configure Vault`_
#. `Configure the Ceph Object Gateway`_
#. `Create a key in Vault`_
#. `Upload object`_

Configure Vault
===============

Vault provides several Secret Engines, which can store, generate, and encrypt
data. Currently, the Object Gateway supports `KV Secrets engine`_ version 2
an `KV Transit engine`_.

Basic Vault Configuration
-------------------------
To enable the KV engine version 2 in Vault, use the Vault command line
tool::

  vault secrets enable kv-v2

Analogously for the Transit Engine::
  vault secrets enable transit

Vault also provides several authentication mechanisms.
To simplify user's interaction with Vault, the Object Gateway supports
two modes: `token authentication method`_ and `agent authentication method`_.

When authenticating using the token method, a token must be obtained
for the Gateway and saved in a file as plain-text.

For security reasons, the Object Gateway should be given a Vault token with a
restricted policy that allows it to fetch secrets only. Such a policy can be
created in Vault using the Vault command line utility::

  vault policy write rgw-policy -<<EOF
    path "secret/data/*" {
      capabilities = ["read"]
    }
  EOF

A token with the policy above can be created by a Vault administrator as
follows::

  vault token create -policy=rgw-policy

Sample output::

  Key                  Value
  ---                  -----
  token                s.72KuPujbc065OdWB71poOmIq
  token_accessor       jv95ZYBUFv6Ss84x7SCSy6lZ
  token_duration       768h
  token_renewable      true
  token_policies       ["default" "rgw-policy"]
  identity_policies    []
  policies             ["default" "rgw-policy"]

The actual token, displayed in the output of the above command, must be saved
in a file as plain-text. The path to this file must then be provided in the
Gateway configuration file (see section below). For security reasons, ensure
the file is readable by the Object Gateway only.


Configure the Ceph Object Gateway
=================================

Edit the Ceph configuration file to enable Vault as a KMS for server-side
encryption. The following example uses the ``token`` authentication method (with
a Vault token stored in a file), sets the Vault server address, and restricts
the URLs where encryption keys can be retrieved from Vault using a path prefix::

   rgw crypt s3 kms backend = vault
   rgw crypt vault auth = token
   rgw crypt vault addr = http://vaultserver:8200
   rgw crypt vault prefix = /v1/secret/data
   rgw crypt vault token file = /etc/ceph/vault.token

In this example, the Gateway will only fetch encryption keys under
``http://vaultserver:8200/v1/secret/data``.

Create a key in Vault
=====================

.. note:: Server-side encryption keys must be 256-bit long and base64 encoded.

To create a key in the KV version 2 engine using Vault's command line tool,
use the commands below::

  export VAULT_ADDR='http://vaultserver:8200'
  vault kv put secret/myproject/mybucketkey key=$(openssl rand -base64 32)

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

Upload object
=============

When uploading an object, provide the SSE key ID in the request. As an example,
using the AWS command-line client::

  aws --endpoint=http://radosgw:8000 s3 cp plaintext.txt s3://mybucket/encrypted.txt --sse=aws:kms --sse-kms-key-id myproject/mybucketkey

The Object Gateway will fetch the key from Vault, encrypt the object and store
it in the bucket. Any request to downlod the object will require the correct key
ID for the Gateway to successfully decrypt it.

Note that the secret will be fetched from Vault using a URL constructed by
concatenating the base address (``rgw crypt vault addr``), the (optional)
URL prefix (``rgw crypt vault prefix``), and finally the key ID. In the example
above, the Gateway will fetch the secret from::

  http://vaultserver:8200/v1/secret/data/myproject/mybucketkey

.. _Server-Side Encryption: ../encryption
.. _Vault: https://www.vaultproject.io/docs/
.. _token authentication method: https://www.vaultproject.io/docs/auth/token.html
.. _KV Secrets engine: https://www.vaultproject.io/docs/secrets/kv/
