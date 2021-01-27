========================
 External Authentication
========================

It is possible to use a custom authentication server for Ceph Object Gateway.
With this option you can use your own authentication server to authenticate
users with all RGW User features like SubUser, Tenant, Permissions.
The only thing you should obey is to implement these APIs so we can get needed
data from your sever.

The following configuration options are available for external authentication::

	[client.radosgw.gateway]
	rgw s3 auth use external authentication = true
	rgw s3 external authentication auth endpoint = {endpoint for getting user info}
	rgw s3 external authentication secret endpoint = {endpoint for getting user secret key}
	rgw s3 external authentication token = {external authentication server baerer token}
	rgw s3 external authentication verify ssl = {verify external authentication server ssl certificate}

External Authentication APIs
============================

In your authentication server you should implement these APIs so we can get our needed data
for authenticating users.

Authentication Endpoint
-----------------------

This API is for authenticating users based on their access key id and request signature.

Example request::

   POST /auth-endpoint HTTP/1.1
   Host: ea.example.com
   Content-Type: application/json
   X-Auth-Token: TOKEN
   
   {
       "credentials": {
           "access_key_id": "REQUEST_ACCESS_KEY",
           "signature": "REQUEST_SIGNATURE"
       }
   }

``access_key_id``: This is a access key which is used in request.

``signature``: This is request signature which you should assert it with user secret key

Response::

   {
       "user_id": "username",
       "tenant": "USER_TENANT",
       "user_name": "FULL NAME",
       "is_admin": true,
       "subuser": {
           "id": "user_id:subuser",
           "permissions": "full-control"
       }
   }

``user_id``: user id for RGW User

``tenant``: Optional. If user has tenant.

``user_name``: display name in RGW User

``is_admin``: Optional. default is false. true if user is admin.

``subuser``: Optional. If user is subuser of a user. ``id`` should be in a format of ``user_id:subuser``

**Response status codes:**
    - `200` if user is valid
    - `401` if signature is invalid
    - `404` if access key not found

Secret Endpoint
-----------------------

This API is for getting secret for access key provided in query params for caching.

Example request::

   GET /secret-endpoint?access_key_id=ACCESS_KEY_ID HTTP/1.1
   Host: ea.example.com
   Content-Type: application/json
   X-Auth-Token: TOKEN

``access_key_id``: This is a access key which needed a secret for it

Response::

   {
       "secret": "SECRET_KEY"
   }

``secret``: secret key for ``access_key_id``

**Response status codes:**
    - `200` if access key is valid
    - `404` if access key not found
