===============================
 OpenID Connect Provider in RGW
===============================

An entity describing the OpenID Connect Provider needs to be created in RGW, in order to establish trust between the two.

REST APIs for Manipulating an OpenID Connect Provider
=====================================================

The following REST APIs can be used for creating and managing an OpenID Connect Provider entity in RGW.

In order to invoke the REST admin APIs, a user with admin caps needs to be created.

.. code-block:: javascript

  radosgw-admin --uid TESTER --display-name "TestUser" --access_key TESTER --secret test123 user create
  radosgw-admin caps add --uid="TESTER" --caps="oidc-provider=*"


CreateOpenIDConnectProvider
---------------------------------

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
---------------------------

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
----------------------------------

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
