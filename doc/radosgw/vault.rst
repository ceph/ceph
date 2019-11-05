===========================
HashiCorp Vault Integration
===========================

HashiCorp `Vault`_ can be used as a secure key management service for
`Server-Side Encryption`_ (SSE-KMS).

#. `Vault Access`_
#. `Secrets Engines`_
#. `Configure Ceph Object Gateway`_
#. `Upload object`_

Vault Access
============

Access to Vault can be done through several authentication mechanisms.
Object Gateway supports `token authentication method`_ and `vault agent`_.

Token Authentication
--------------------
The Token authentication method expects a Vault token to be present in a plaintext file. You can configure
the token option in Ceph's configuration file as follows::

     rgw crypt vault auth = token
     rgw crypt vault token file = /etc/ceph/vault.token

For security reasons, ensure the file is readable by the Object Gateway only.

.. note:: Token authentication is not recommended configuration for production environments.

Vault Agent
-----------
Vault Agent is a client daemon that provides authentication to Vault, manages the token renewal and caching.
With a Vault agent, it is possible to use other Vault authentication mechanism such as AppRole, AWS, Certs, JWT, Azure...

You can enable agent authentication in Ceph's configuration file with::

     rgw crypt vault auth = agent
     rgw crypt vault addr = http://localhost:8200

.. note:: ``rgw_crypt_vault_token_file`` is ignored if ``rgw_crypt_vault_auth`` is set to ``agent``.

Secrets Engines
===============
Vault provides several Secret Engines, which can store, generate, and encrypt data. Currently, the Object Gateway supports:

- `KV Secrets engine`_ version 2
- `Transit engine`_

.. important:: Examples in this section are for demonstration purposes only and assume token authentication.


KV Secrets engine
-----------------
To enable the KV engine version 2 in Vault, use the Vault command line
tool::

  vault secrets enable kv-v2

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


Create a key using KV Engine
----------------------------

.. note:: Server-side encryption keys must be 256-bit long and base64 encoded.

To create a key in the KV version 2 engine using Vault's command line tool,
use the commands below::

  export VAULT_ADDR='http://vaultserver:8200'
  vault kv put secret/myproject/mybucketkey key=$(openssl rand -base64 32)

.. important:: In the KV secrets engine, secrets are stored as key-value pairs, and the Gateway expects the key name to be key, i.e. the secret must be in the form key=<value>.

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
  key    Ak5dRyLQjwX/wb7vo6Fq1qjsfk1dh2CiSicX-gLAhwk=


Transit Secrets Engine
----------------------
To enable the Transit engine in Vault, use the Vault command line tool::

  vault secrets enable transit

Create a key using Transit secrets engine
-----------------------------------------
Object Gateway supports Transit engine exportable keys only.
To create an exportable key, use the command line tool::

   export VAULT_ADDR='http://vaultserver:8200'
   vault write -f transit/keys/mybucketkey exportable=true

The command line above creates a keyring, which contains an aes256-gcm96 key type.
To verify the key is created properly, use the command line tool::

  vault read transit/export/encryption-key/mybucketkey/1

Output::

  Key     Value
  ---     -----
  keys    map[1:-gbTI9lNpqv/V/2lDcmH2Nq1xKn6FPDWarCmFM2aNsQ=]
  name    mybucketkey
  type    aes256-gcm96

.. note:: To use Transit Secrets engine in Object Gateway, you shall specify the full key, including its version.

For security reasons, the Object Gateway should be given a Vault token with a
restricted policy that allows it to fetch the dedicated keyrings only. Such a policy can be
created in Vault using the Vault command line utility::

  vault policy write rgw-transit-policy -<<EOF
    path "transit/export/encryption-key/mybucketkey/*" {
      capabilities = ["read"]
    }
  EOF

Once the policy is created, a token can be generated by Vault administrator::

  vault token create -policy=rgw-transit-policy

Sample output::

  Key                  Value
  ---                  -----
  token                s.62KuPujbc1234dWB71poOmIZ
  token_accessor       jv95ZYBUFv6Ss84x7SCSy6lZ
  token_duration       768h
  token_renewable      true
  token_policies       ["default" "rgw-transit-policy"]
  identity_policies    []
  policies             ["default" "rgw-transit-policy"]


Configure Ceph Object Gateway
=================================

Edit the Ceph configuration file to enable Vault as a KMS for server-side
encryption.::

   rgw crypt s3 kms backend = vault
   rgw crypt vault auth = { token | agent }
   rgw crypt vault addr = { vault address e.g. http://localhost:8200 }
   rgw crypt vault prefix = { prefix to secret engine e.g. /v1/transit/export/encryption-key }
   rgw crypt vault token file = { absolute path to token file e.g. /etc/ceph/vault.token }

The following example uses the ``token`` authentication method (with
a Vault token stored in a file), sets the Vault server address, and restricts
the URLs where encryption keys can be retrieved from Vault using a path prefix.
In this example, the Gateway will only fetch transit encryption keys::

   rgw crypt s3 kms backend = vault
   rgw crypt vault auth = token
   rgw crypt vault addr = https://vaultserver
   rgw crypt vault secret engine = transit
   rgw crypt vault prefix = /v1/transit/export/encryption-key
   rgw crypt vault token file = /etc/ceph/vault.token


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
.. _vault agent: https://www.vaultproject.io/docs/agent/index.html
.. _KV Secrets engine: https://www.vaultproject.io/docs/secrets/kv/
.. _Transit engine: https://www.vaultproject.io/docs/secrets/transit
