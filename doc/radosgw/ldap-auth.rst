===================
LDAP Authentication
===================

You can delegate the Ceph Object Gateway authentication to an LDAP server.

How it works
============

The Ceph Object Gateway extracts the user's LDAP credentials from a token. A
search filter is constructed with the user name. The Ceph Object Gateway uses
the configured service account to search the directory for a matching entry. If
an entry is found, the Ceph Object Gateway attempts to bind to the found
distinguished name with the password from the token. If the credentials are
valid, the bind will succeed, and the Ceph Object Gateway will grant access and
radosgw-user will be created with the provided username.

You can limit the allowed users by setting the base for the search to a
specific organizational unit or by specifying a custom search filter, for
example requiring specific group membership, custom object classes, or
attributes.

The LDAP credentials must be available on the server to perform the LDAP
authentication. Make sure to set the ``rgw`` log level low enough to hide the
base-64-encoded credentials / access tokens.

Requirements
============

- **LDAP or Active Directory:** A running LDAP instance accessible by the Ceph
  Object Gateway
- **Service account:** LDAP credentials to be used by the Ceph Object Gateway
  with search permissions
- **User account:** At least one user account in the LDAP directory
- **Do not overlap LDAP and local users:** You should not use the same user
  names for local users and for users being authenticated by using LDAP. The
  Ceph Object Gateway cannot distinguish them and it treats them as the same
  user.

Sanity checks
=============

Use the ``ldapsearch`` utility to verify the service account or the LDAP connection:

::

  # ldapsearch -x -D "uid=ceph,ou=system,dc=example,dc=com" -W \
  -H ldaps://example.com -b "ou=users,dc=example,dc=com" 'uid=*' dn

.. note:: Make sure to use the same LDAP parameters like in the Ceph configuration file to
          eliminate possible problems.

Configuring the Ceph Object Gateway to use LDAP authentication
==============================================================

The following parameters in the Ceph configuration file are related to the LDAP
authentication:

- ``rgw_s3_auth_use_ldap``: Set this to ``true`` to enable S3 authentication with LDAP
- ``rgw_ldap_uri``:  Specifies the LDAP server to use. Make sure to use the
  ``ldaps://<fqdn>:<port>`` parameter to not transmit clear text credentials
  over the wire.
- ``rgw_ldap_binddn``: The Distinguished Name (DN) of the service account used
  by the Ceph Object Gateway
- ``rgw_ldap_secret``: Path to file containing credentials for ``rgw_ldap_binddn``
- ``rgw_ldap_searchdn``: Specifies the base in the directory information tree
  for searching users. This might be your users organizational unit or some
  more specific Organizational Unit (OU).
- ``rgw_ldap_dnattr``: The attribute being used in the constructed search
  filter to match a username. Depending on your Directory Information Tree
  (DIT) this would probably be ``uid`` or ``cn``. The generated filter string
  will be, e.g., ``cn=some_username``.
- ``rgw_ldap_searchfilter``: If not specified, the Ceph Object Gateway
  automatically constructs the search filter with the ``rgw_ldap_dnattr``
  setting. Use this parameter to narrow the list of allowed users in very
  flexible ways. Consult the *Using a custom search filter to limit user access
  section* for details

Using a custom search filter to limit user access
=================================================

There are two ways to use the ``rgw_ldap_searchfilter`` parameter:

Specifying a partial filter to further limit the constructed search filter
--------------------------------------------------------------------------

An example for a partial filter:

::

  "objectclass=inetorgperson"

The Ceph Object Gateway will generate the search filter as usual with the
user name from the token and the value of ``rgw_ldap_dnattr``. The constructed
filter is then combined with the partial filter from the ``rgw_ldap_searchfilter``
attribute. Depending on the user name and the settings the final search filter
might become:

::

  "(&(uid=hari)(objectclass=inetorgperson))"

So user ``hari`` will only be granted access if he is found in the LDAP
directory, has an object class of ``inetorgperson``, and did specify a valid
password.

Specifying a complete filter
----------------------------

A complete filter must contain a ``@USERNAME@`` token which will be substituted
with the user name during the authentication attempt. The ``rgw_ldap_dnattr``
parameter is not used anymore in this case. For example, to limit valid users
to a specific group, use the following filter:

::

  "(&(uid=@USERNAME@)(memberOf=cn=ceph-users,ou=groups,dc=mycompany,dc=com))"

.. note:: Using the ``memberOf`` attribute in LDAP searches requires server-side
          support from your specific LDAP server implementation.

Generating an access token for LDAP authentication
==================================================

The ``radosgw-token`` utility generates the access token based on the LDAP
user name and password. It will output a base-64 encoded string which is the
access token.

::

  # export RGW_ACCESS_KEY_ID="<username>"
  # export RGW_SECRET_ACCESS_KEY="<password>"
  # radosgw-token --encode --ttype=ldap

.. important:: The access token is a base-64 encoded JSON struct and contains
               the LDAP credentials as a clear text.

Alternatively, users can also generate the token manually by base-64-encoding
this JSON snippet, if they do not have the ``radosgw-token`` tool installed.

::

  {
    "RGW_TOKEN": {
      "version": 1,
      "type": "ldap",
      "id": "your_username",
      "key": "your_clear_text_password_here"
    }
  }

Using the access token
======================

Use your favorite S3 client and specify the token as the access key in your
client or environment variables.

::

  # export AWS_ACCESS_KEY_ID=<base64-encoded token generated by radosgw-token>
  # export AWS_SECRET_ACCESS_KEY="" # define this with an empty string, otherwise tools might complain about missing env variables.

.. important:: The access token is a base-64 encoded JSON struct and contains
               the LDAP credentials as a clear text. DO NOT share it unless
               you want to share your clear text password!

.. _radosgw-ldap-to-sts:

Migrating from LDAP Authentication to STS
=========================================

With LDAP authentication, every S3 request carries the user's LDAP password,
and the Ceph Object Gateway binds to the LDAP server to check it. With the
Secure Token Service (STS), users authenticate with an OpenID Connect (OIDC)
identity provider that is backed by the same directory, and then call
``AssumeRoleWithWebIdentity`` to get temporary S3 credentials (see
:doc:`STS`). LDAP authentication and STS can be enabled at the same time, so
clients can be moved one at a time.

How Role Sessions Differ from LDAP Users
----------------------------------------

The first time an LDAP user authenticates, the Ceph Object Gateway creates a
user whose ID is the LDAP user name. ``radosgw-admin user info`` shows this
user with ``"type": "ldap"`` and no keys. This user owns the buckets and
objects that the LDAP user creates.

Requests made with the temporary credentials from
``AssumeRoleWithWebIdentity`` are not made by that user:

- Only IAM policies can grant a role session access: the permission policies
  of the role, and bucket policies. ACL grants do not apply to role sessions.
- The Ceph Object Gateway creates a shadow user for each federated user, with
  the ID ``<tenant>$oidc$<sub>``, where ``<sub>`` is the ``sub`` claim of the
  web token. The identity provider chooses this claim, and it is usually not
  the LDAP user name. Keycloak, for example, uses its own ID for the user.
- Buckets that a role session creates, and objects that it writes, are owned
  by the shadow user.
- ``ListBuckets`` requires ``s3:ListAllMyBuckets`` in a policy of the role,
  and lists only the buckets that the shadow user owns.

Migration Procedure
-------------------

#. Set up an OIDC identity provider that authenticates users against the LDAP
   directory, for example Keycloak with LDAP user federation (see
   :ref:`radosgw_keycloak`). Create a client for the Ceph Object Gateway. The
   ``aud`` claim of the tokens must contain the ID of the client, or the
   ``azp`` claim must match it. In Keycloak, add an audience mapper to the
   client so that ``aud`` contains its ID.

#. Enable STS on every Ceph Object Gateway in the zone, and leave LDAP
   authentication enabled for now. All of the gateways in a zone must use the
   same :confval:`rgw_sts_key`:

   .. prompt:: bash #

      ceph config set client.rgw rgw_sts_key $(ceph-authtool --gen-print-key)
      ceph config set client.rgw rgw_s3_auth_use_sts true

   Then restart the gateways.

#. Give an RGW user the capabilities to manage OIDC providers and roles:

   .. prompt:: bash #

      radosgw-admin caps add --uid=<admin-user> --caps="oidc-provider=*;roles=*"

#. Register the identity provider as an OIDC provider (see :doc:`oidc`). The
   URL must match the ``iss`` claim of the tokens. Unless
   ``rgw_enable_jwks_url_verification`` is enabled, the thumbprint is the
   SHA-1 fingerprint of the certificate in the ``x5c`` field of the signing
   key of the provider. For a Keycloak realm, for example:

   .. prompt:: bash $

      curl -s https://keycloak.example.com/realms/example/protocol/openid-connect/certs \
        | jq -r '.keys[] | select(.use == "sig" and .alg == "RS256") | .x5c[0]' \
        | base64 -d | openssl x509 -inform der -noout -fingerprint -sha1 \
        | sed 's/.*=//; s/://g'
      aws --endpoint-url https://rgw.example.com iam create-open-id-connect-provider \
        --url https://keycloak.example.com/realms/example \
        --client-id-list ceph --thumbprint-list <thumbprint>

   When the provider rotates its signing key, add the thumbprint of the new
   certificate. A provider can have up to five thumbprints.

#. Create a role for each LDAP user, or for each group of users that share
   buckets (see :doc:`role`). In the trust policy, require the client ID in
   ``app_id`` and require the claim that carries the LDAP user name, which in
   Keycloak is ``preferred_username``. Each condition key begins with the URL
   of the provider without its scheme. For the LDAP user ``alice``:

   .. code-block:: json

      {
        "Version": "2012-10-17",
        "Statement": [{
          "Effect": "Allow",
          "Principal": {"Federated": ["arn:aws:iam:::oidc-provider/keycloak.example.com/realms/example"]},
          "Action": ["sts:AssumeRoleWithWebIdentity"],
          "Condition": {"StringEquals": {
            "keycloak.example.com/realms/example:app_id": "ceph",
            "keycloak.example.com/realms/example:preferred_username": "alice"
          }}
        }]
      }

   In a permission policy of the role, allow access to the buckets of the
   LDAP user:

   .. code-block:: json

      {
        "Version": "2012-10-17",
        "Statement": [
          {"Effect": "Allow", "Action": ["s3:*"],
           "Resource": ["arn:aws:s3:::alice-bucket", "arn:aws:s3:::alice-bucket/*"]},
          {"Effect": "Allow", "Action": ["s3:ListAllMyBuckets"], "Resource": ["*"]}
        ]
      }

   The permission policies of a role apply only to buckets in the tenant of
   the role. To grant a role access to a bucket in another tenant, add a
   bucket policy that names the role.

#. Move the clients one at a time. Each client gets a token from the identity
   provider, calls ``AssumeRoleWithWebIdentity``, and uses the temporary
   credentials, getting new ones before they expire:

   .. prompt:: bash $

      aws --endpoint-url https://rgw.example.com sts assume-role-with-web-identity \
        --role-arn arn:aws:iam:::role/alice --role-session-name alice \
        --web-identity-token "$TOKEN"

#. Optionally, make the shadow user the owner of the buckets, so that they
   appear in ``ListBuckets``. The shadow user exists after the first
   ``AssumeRoleWithWebIdentity`` call of the user; ``radosgw-admin user list``
   shows its ID. ``bucket chown`` rewrites the ACL of every object in the
   bucket, which takes time for a large bucket:

   .. prompt:: bash #

      radosgw-admin bucket link --bucket=alice-bucket --uid='$oidc$<sub>'
      radosgw-admin bucket chown --bucket=alice-bucket --uid='$oidc$<sub>'

#. When no client uses LDAP tokens anymore, disable LDAP authentication and
   restart the gateways:

   .. prompt:: bash #

      ceph config set client.rgw rgw_s3_auth_use_ldap false

   The gateways then refuse requests that use LDAP tokens. The users that were
   created for LDAP users remain, and they still own their buckets and objects
   unless you changed the owners.
