================================
 OpenID Connect Provider in RGW
================================

An entity describing the OpenID Connect Provider needs to be created in RGW, in order to establish trust between the two.

OIDC providers can be created at two scopes:

- **Account-level**: Created via the IAM REST API, scoped to a specific account.
- **Global**: Created via ``radosgw-admin``, available to all accounts as a fallback.

When ``AssumeRoleWithWebIdentity`` is called, RGW first looks for an OIDC provider
in the role's account. If none is found, it falls back to the global OIDC provider
with the same URL. Account-level providers always take precedence over global ones.

.. note::

   Global OIDC providers are only supported with account-based identities, not with
   legacy tenant-based namespaces.


Global OIDC Providers via radosgw-admin
=======================================

Global OIDC providers are managed via ``radosgw-admin`` and are available to all
accounts without requiring each account to register the provider individually.

Create a Global OIDC Provider
-----------------------------

.. prompt:: bash #

   radosgw-admin oidc-provider create --provider-url https://accounts.google.com \
     --client-ids app1,app2 --thumbprints F7D7B3515DD0D319DD219A43A9EA727AD6065287

If ``--account-id`` is specified, the provider is created in that account's scope
instead of globally. Account-scoped providers should normally be created via the
IAM REST API (``CreateOpenIDConnectProvider``); the ``radosgw-admin`` command is
intended for managing global providers.

Get a Global OIDC Provider
--------------------------

.. prompt:: bash #

   radosgw-admin oidc-provider get --provider-url https://accounts.google.com

Modify a Global OIDC Provider
------------------------------

Update thumbprints, client-ids, or both. The provided list fully replaces the
existing list for that field; unspecified fields are left unchanged. To add a
single entry, include the existing values along with the new one.

.. prompt:: bash #

   radosgw-admin oidc-provider modify --provider-url https://accounts.google.com \
     --thumbprints NEWTHUMB1,NEWTHUMB2

   radosgw-admin oidc-provider modify --provider-url https://accounts.google.com \
     --client-ids newapp1,newapp2

Delete a Global OIDC Provider
-----------------------------

.. prompt:: bash #

   radosgw-admin oidc-provider delete --provider-url https://accounts.google.com

List Global OIDC Providers
--------------------------

.. prompt:: bash #

   radosgw-admin oidc-provider list


Trust Policy for Roles Using OIDC Providers
===========================================

A role's trust policy must reference the OIDC provider in the ``Principal.Federated``
field. The format differs depending on whether the role uses a global or account-scoped
OIDC provider.

Trust Policy for Global OIDC Providers
--------------------------------------

When using a global OIDC provider, the trust policy uses the **bare provider URL**
as the federated principal::

  {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "Federated": ["<provider-url>"]
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "<provider-url>:sub": "<allowed-subject>"
        }
      }
    }]
  }

For example, if the global OIDC provider URL is
``localhost:8080/auth/realms/demorealm``::

  "Principal": {"Federated": ["localhost:8080/auth/realms/demorealm"]}

Trust Policy for Account-Scoped OIDC Providers
----------------------------------------------

When using an account-scoped OIDC provider (created via the S3 IAM API), the trust
policy uses the full **ARN** as the federated principal::

  {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "Federated": ["arn:aws:iam::<account-id>:oidc-provider/<provider-url>"]
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "<provider-url>:sub": "<allowed-subject>",
          "<provider-url>:app_id": "<allowed-client-id>"
        }
      }
    }]
  }

The ``<account-id>`` in the ARN must match the account that owns the role.

.. note::

   The trust policy principal format must match the provider scope:

   - A role using a **global** provider must use the bare URL form.
   - A role using an **account-scoped** provider must use the ARN form.

   If there is a mismatch (for example, ARN form with a non-matching account while the
   global provider is loaded), the trust policy evaluation will deny the request.

   The global OIDC provider does not bypass trust policy evaluation. Every role must
   still have a trust policy that explicitly allows the OIDC provider. The global
   provider only determines where the provider configuration (thumbprints, client IDs)
   is loaded from during JWT signature validation.


REST APIs for Manipulating an OpenID Connect Provider
=====================================================

The following REST APIs can be used for creating and managing an OpenID Connect Provider entity in RGW.

In order to invoke the REST admin APIs, a user with admin caps needs to be created.

.. prompt:: bash #

   radosgw-admin --uid TESTER --display-name "TestUser" --access_key TESTER --secret test123 user create
   radosgw-admin caps add --uid="TESTER" --caps="oidc-provider=*"


CreateOpenIDConnectProvider
---------------------------

Create an OpenID Connect Provider entity in RGW

Request Parameters
~~~~~~~~~~~~~~~~~~

``ClientIDList.member.N``

:Description: List of Client Ids that needs access to S3 resources.
:Type: Array of Strings

``ThumbprintList.member.N``

:Description: List of OpenID Connect IDP's server certificates' thumbprints. A maximum of 5 thumbprints are allowed.
:Type: Array of Strings

``Url``

:Description: URL of the IDP.
:Type: String


Example::

  POST "<hostname>?Action=Action=CreateOpenIDConnectProvider
    &ThumbprintList.list.1=F7D7B3515DD0D319DD219A43A9EA727AD6065287
    &ClientIDList.list.1=app-profile-jsp
    &Url=http://localhost:8080/auth/realms/quickstart"


DeleteOpenIDConnectProvider
---------------------------

Deletes an OpenID Connect Provider entity in RGW

Request Parameters
~~~~~~~~~~~~~~~~~~

``OpenIDConnectProviderArn``

:Description: ARN of the IDP which is returned by the Create API.
:Type: String

Example::

  POST "<hostname>?Action=Action=DeleteOpenIDConnectProvider
    &OpenIDConnectProviderArn=arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"


GetOpenIDConnectProvider
------------------------

Gets information about an IDP.

Request Parameters
~~~~~~~~~~~~~~~~~~

``OpenIDConnectProviderArn``

:Description: ARN of the IDP which is returned by the Create API.
:Type: String

Example::

  POST "<hostname>?Action=Action=GetOpenIDConnectProvider
    &OpenIDConnectProviderArn=arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart"

ListOpenIDConnectProviders
--------------------------

Lists information about all IDPs

Request Parameters
~~~~~~~~~~~~~~~~~~

None

Example::

  POST "<hostname>?Action=Action=ListOpenIDConnectProviders

AddClientIDToOpenIDConnectProvider
----------------------------------

Add a client id to the list of existing client ids registered while creating an OpenIDConnectProvider.

Request Parameters
~~~~~~~~~~~~~~~~~~

``OpenIDConnectProviderArn``

:Description: ARN of the IDP which is returned by the Create API.
:Type: String

``ClientID``

:Description: Client Id to add to the existing OpenIDConnectProvider.
:Type: String

Example::

  POST "<hostname>?Action=Action=AddClientIDToOpenIDConnectProvider
    &OpenIDConnectProviderArn=arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart
    &ClientID=app-jee-jsp"

RemoveClientIDFromOpenIDConnectProvider
---------------------------------------

Remove a client id from the list of existing client ids registered while creating an OpenIDConnectProvider.

Request Parameters
~~~~~~~~~~~~~~~~~~

``OpenIDConnectProviderArn``

:Description: ARN of the IDP which is returned by the Create API.
:Type: String

``ClientID``

:Description: Client ID to remove from the existing OpenIDConnectProvider.
:Type: String

Example::

  POST "<hostname>?Action=Action=RemoveClientIDFromOpenIDConnectProvider
    &OpenIDConnectProviderArn=arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart
    &ClientID=app-jee-jsp"

UpdateOpenIDConnectProviderThumbprint
-------------------------------------

Update the existing thumbprint list of an OpenIDConnectProvider with the given list.
This API removes the existing thumbprint list and replaces that with the input thumbprint list.

Request Parameters
~~~~~~~~~~~~~~~~~~

``OpenIDConnectProviderArn``

:Description: ARN of the IDP which is returned by the Create API.
:Type: String

``ThumbprintList.member.N``

:Description: List of OpenID Connect IDP's server certificates' thumbprints. A maximum of 5 thumbprints are allowed.
:Type: Array of Strings

Example::

  POST "<hostname>?Action=Action=UpdateOpenIDConnectProviderThumbprint
    &OpenIDConnectProviderArn=arn:aws:iam:::oidc-provider/localhost:8080/auth/realms/quickstart
    &&ThumbprintList.list.1=ABCDB3515DD0D319DD219A43A9EA727AD6061234"
