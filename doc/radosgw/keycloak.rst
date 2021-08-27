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

Several examples of apps authenticating with Keycloak are given here: https://github.com/keycloak/keycloak-quickstarts/blob/latest/docs/getting-started.md
Taking the example of app-profile-jee-jsp app given in the link above, its client secret and client password, can be used to fetch the
access token (web token) as given below::

    KC_REALM=demo
    KC_CLIENT=<client id>
    KC_CLIENT_SECRET=<client secret>
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

Example showing how to fetch a web token from Keycloak using Kerberos or Username and Password authentication
=============================================================================================================

The following python script takes various command line arguments to authenticate against Keycloak via Kerberos or Basic authentication to obtain a Java Web 
Token. This token can then be exchanged for temporary credentials with the RADOS gateway in order to AssumeRoleWithWebIdentity:

.. code-block:: python
    #!/usr/bin/env python
    import json,requests,urllib,re,argparse,sys,os,boto3
    from requests_kerberos import HTTPKerberosAuth, OPTIONAL
    from lxml import html
    from getpass import getpass
    
    def get_token(client,client_secret,redirect,base_url,auth_uri,token_uri,method,verify_ssl,user,password):
        session = requests.session()
        if method == 'kerberos':
          kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL, force_preemptive=True)
          access_code_params = {'client_id': client, 'response_type': 'code', 'scope': 'openid', 'redirect_uri': redirect}
          try:
            get_auth_response = session.get(base_url + auth_uri + "?" + urllib.parse.urlencode(access_code_params), verify=verify_ssl, auth=kerberos_auth, allow_redirects=False)
          except Exception as e:
            print("Something went wrong with auth")
            print(e)
            session.close()
            sys.exit(1)
          try:
            matches = re.search('&code=(.*?)$', get_auth_response.headers['Location'])
            code = matches.group(1)
            token_params = {'code': code, 'grant_type': 'authorization_code', 'client_id': client, 'client_secret': client_secret, 'redirect_uri': redirect}
            token_response = session.post(base_url + token_uri, token_params, allow_redirects=False, verify=verify_ssl)
            return token_response.text
          except Exception as e:
            print("Something went wrong obtaining the JWT")
            print(e)
            session.close()
            sys.exit(1)
          finally:
            session.close()
        if method == 'password':
          if user is None:
            user = input('username: ')
          if password is None:
            password = getpass()
          token_params = {'grant_type': 'password', 'client_id': client, 'client_secret': client_secret, 'redirect_uri': redirect, 'username': user, 'password': password, 'scope': 'openid'}
          try:
            token_response = session.post(base_url + token_uri, token_params, allow_redirects=False, verify=verify_ssl)
            return token_response.text
          except Exception as e:
            print("Something went wrong obtaining the JWT")
            print(e)
            session.close()
            sys.exit(1)
          finally:
            session.close()
    
    def main():
      parser = argparse.ArgumentParser(description='Obtain JWT using Kerberos or Password Auth from KeyCloak server and optionally exchange for temporary S3 credentials for RADOS gateway.')
      parser.add_argument('-c','--client', help="RH-SSO Client name", required=True)
      parser.add_argument('-s','--client-secret', help="RH-SSO Client secret", required=True)
      parser.add_argument('-e','--rgw-endpoint', help="Ceph RGW endpoint (redirect_url)", required=True)
      parser.add_argument('-b','--base-url', help="RH-SSO URL", required=True)
      parser.add_argument('-a','--auth-uri', help="RH-SSO auth endpoint (default '/auth')", default='/auth')
      parser.add_argument('-t','--token-uri', help="RH-SSO token endpoint (default '/token')", default='/token')
      parser.add_argument('-m','--method', choices=['kerberos','password'], help="Authentication method, either kerberos or password (default 'kerberos')", default='kerberos')
      parser.add_argument('-u','--user', help="Username, for use with 'password' method.")
      parser.add_argument('-p','--password', help="Password, for use with 'password' method.")
      parser.add_argument('-k','--verify-ssl', help="Verify SSL - can be either True/False/path to CA certificate (default 'True')", default=True)
      parser.add_argument('-r','--role-arn', help="Role arn to assume")
      parser.add_argument('-d','--duration', help="Seconds creds are valid for. Default is 3600 (1h), maximum is 43200 (12h) - note this is configurable in Ceph so restrictions may differ.", default=3600)
      args = parser.parse_args()
      token = get_token(args.client,args.client_secret,args.rgw_endpoint,args.base_url,args.auth_uri,args.token_uri,args.method,args.verify_ssl,args.user,args.password)
      parsed_token = json.loads(token)
      for output in ['id_token','access_token','refresh_token']:
        print("export KC_" + output.upper() +"=" + parsed_token[output])
      if args.role_arn is not None:
        role_session_name = os.getenv('USER')
        parsed_token = json.loads(token)
        sts_client = boto3.client(
           'sts',
           aws_access_key_id="",
           aws_secret_access_key="",
           endpoint_url=args.rgw_endpoint.strip("/"),
           region_name='',
        )
    
        response = sts_client.assume_role_with_web_identity(
          RoleArn=args.role_arn,
          RoleSessionName=role_session_name,
          DurationSeconds=args.duration,
          WebIdentityToken=parsed_token['id_token'],
        )
        print("export AWS_ACCESS_KEY_ID=" + response['Credentials']['AccessKeyId'])
        print("export AWS_SECRET_ACCESS_KEY=" + response['Credentials']['SecretAccessKey'])
        print("export AWS_SESSION_TOKEN=" + response['Credentials']['SessionToken'])
    
    if __name__ == "__main__":
        main()

Related github repository; https://github.com/rh-jpoole/kc-krb-jwt.