=================================
Keycloak integration with RadosGW
=================================

Keycloak can be setup as an OpenID Connect Identity Provider, which can be used by mobile/ web apps
to authenticate their users. The Web token returned as a result of authentication can be used by the
mobile/ web app to call AssumeRoleWithWebIdentity to get back a set of temporary S3 credentials,
which can be used by the app to make S3 calls.

Setting up Keycloak
====================

Installing and bringing up Keycloak can be found here: https://www.keycloak.org/docs/latest/server_installation/.

Configuring Keycloak to talk to RGW
===================================

The following configurables have to be added for RGW to talk to Keycloak. 
The format of token inspection url is https://[base-server-url]/token/introspect::

  [client.radosgw.gateway]
  rgw sts key = {sts key for encrypting/ decrypting the session token}
  rgw s3 auth use sts = true

Example showing how to fetch a web token from Keycloak
======================================================

Several examples of apps authenticating with Keycloak are given here: https://github.com/keycloak/keycloak/tree/master/examples/demo-template
Taking the example of customer-portal app given in the link above, its client secret and client password, can be used to fetch the
access token (web token) as given below::

    KC_REALM=demo
    KC_CLIENT=customer-portal
    KC_CLIENT_SECRET=password
    KC_SERVER=<host>:8080
    KC_CONTEXT=auth

    # Request Tokens for credentials
    KC_RESPONSE=$( \
    curl -k -v -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "scope=openid" \
    -d "grant_type=client_credentials" \
    -d "client_id=$KC_CLIENT" \
    -d "client_secret=$KC_CLIENT_SECRET" \
    "http://$KC_SERVER/$KC_CONTEXT/realms/$KC_REALM/protocol/openid-connect/token" \
    | jq .
    )

    KC_ACCESS_TOKEN=$(echo $KC_RESPONSE| jq -r .access_token)

KC_ACCESS_TOKEN can be used to invoke AssumeRoleWithWebIdentity as given in
:doc:`STS`.
