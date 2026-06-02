.. _radosgw-keystone:

=====================================
 Integrating with OpenStack Keystone
=====================================

It is possible to integrate the Ceph Object Gateway with Keystone, the OpenStack
identity service. This sets up the gateway to accept Keystone as the users
authority. A user that Keystone authorizes to access the gateway will also be
automatically created on the Ceph Object Gateway (if it didn't exist beforehand). A
token that Keystone validates will be considered as valid by the gateway.

The following configuration options are available for Keystone integration::

	[client.radosgw.gateway]
        rgw_keystone_api_version = {keystone api version}
        rgw_keystone_url = {keystone server url:keystone server port}
        rgw_keystone_accepted_roles = {accepted user roles}
        rgw_keystone_token_cache_size = {number of tokens to cache}
        rgw_keystone_implicit_tenants = {true for private tenant for each new user}
        rgw_keystone_admin_user = {keystone service user name}
        rgw_keystone_admin_password = {keystone service user password}
        rgw_keystone_admin_password_path = {keystone service user password path} # preferred
        rgw_keystone_admin_domain = {keystone service user domain}
        rgw_keystone_admin_project = {keystone service project name}

You need to configure credentials to use against the Keystone service. You need
a project (tenant), username, and password credentials to be configured to use
the Keystone v3 API.

The service credentials need to have the ``admin`` and ``service`` roles in
Keystone to allow ``POST /v3/s3tokens`` and
``GET /v3/users/<user>/OS-EC2/<credential>`` requests. You can lock down the
service credentials to only need the ``service`` role and avoid granting the
``admin`` role in Keystone by changing the changing the
``identity:ec2_get_credential`` policy.

A Ceph Object Gateway user is mapped to a Keystone ``project``. A Keystone
user may have multiple roles asssigned, possibly for multiple projects.

When the Ceph Object Gateway processes tokens retrieved from Keystone, it looks
at the project, and the user roles that are assigned to that token, and
accepts/rejects the request according to the
:confval:`rgw_keystone_accepted_roles` configurable.

For compatibility with previous versions of Ceph, it is also
possible to set :confval:`rgw_keystone_implicit_tenants` to either
``s3`` or ``swift``.  This has the effect of splitting
the identity space such that the indicated protocol will
only use implicit tenants, and the other protocol will
never use implicit tenants.  Some older versions of Ceph
only supported implicit tenants with Swift.

Configuring Keystone Service and Endpoints
------------------------------------------

Keystone itself needs to be configured to point to the Ceph Object Gateway as
an object storage endpoint:

.. prompt:: bash #

   openstack service create --name=swift \
                              --description="Swift Service" \
                              object-store

::

  +-------------+----------------------------------+
  | Field       | Value                            |
  +-------------+----------------------------------+
  | description | Swift Service                    |
  | enabled     | True                             |
  | id          | 37c4c0e79571404cb4644201a4a6e5ee |
  | name        | swift                            |
  | type        | object-store                     |
  +-------------+----------------------------------+

.. prompt:: bash #

   openstack endpoint create --region RegionOne \
                               --publicurl   "http://radosgw.example.com:8080/swift/v1" \
                               --adminurl    "http://radosgw.example.com:8080/swift/v1" \
                               --internalurl "http://radosgw.example.com:8080/swift/v1" \
                               swift

::

  +--------------+------------------------------------------+
  | Field        | Value                                    |
  +--------------+------------------------------------------+
  | adminurl     | http://radosgw.example.com:8080/swift/v1 |
  | id           | e4249d2b60e44743a67b5e5b38c18dd3         |
  | internalurl  | http://radosgw.example.com:8080/swift/v1 |
  | publicurl    | http://radosgw.example.com:8080/swift/v1 |
  | region       | RegionOne                                |
  | service_id   | 37c4c0e79571404cb4644201a4a6e5ee         |
  | service_name | swift                                    |
  | service_type | object-store                             |
  +--------------+------------------------------------------+

.. prompt:: bash #

   openstack endpoint show object-store

::

  +--------------+------------------------------------------+
  | Field        | Value                                    |
  +--------------+------------------------------------------+
  | adminurl     | http://radosgw.example.com:8080/swift/v1 |
  | enabled      | True                                     |
  | id           | e4249d2b60e44743a67b5e5b38c18dd3         |
  | internalurl  | http://radosgw.example.com:8080/swift/v1 |
  | publicurl    | http://radosgw.example.com:8080/swift/v1 |
  | region       | RegionOne                                |
  | service_id   | 37c4c0e79571404cb4644201a4a6e5ee         |
  | service_name | swift                                    |
  | service_type | object-store                             |
  +--------------+------------------------------------------+

.. note:: If :ref:`ceph-conf-database` sets the configuration option
	  ``rgw_swift_account_in_url = true``, your ``object-store``
	  endpoint URLs must be set to include the suffix
	  ``/v1/AUTH_%(tenant_id)s`` (instead of just ``/v1``).

The Keystone URL is the Keystone admin RESTful API URL. The admin token is the
token that is configured internally in Keystone for admin requests.

OpenStack Keystone may be terminated with a self-signed SSL certificate. In
order for Ceph Object Gateway to interact with Keystone in such a case, you could
install Keystone's SSL certificate in the node running the Ceph Object Gateway instance. Alternatively,
Ceph Object Gateway could be made to not verify the SSL certificate at all (similar to
OpenStack clients with a ``--insecure`` switch) by setting the value of the
configurable :confval:`rgw_keystone_verify_ssl` to false.


.. _OpenStack Keystone documentation: https://docs.openstack.org/developer/keystone/configuringservices.html#setting-up-projects-users-and-roles

Cross-Project (Tenant) Access
-----------------------------

In order to let a project (earlier called a 'tenant') access buckets belonging
to a different project, the following config option needs to be enabled::

   rgw_swift_account_in_url = true

The Keystone object store endpoint must accordingly be configured to include
the ``AUTH_%(project_id)s`` suffix:

.. prompt:: bash #

   openstack endpoint create --region RegionOne \
                               --publicurl   "http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s" \
                               --adminurl    "http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s" \
                               --internalurl "http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s" \
                               swift

::

  +--------------+--------------------------------------------------------------+
  | Field        | Value                                                        |
  +--------------+--------------------------------------------------------------+
  | adminurl     | http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s |
  | id           | e4249d2b60e44743a67b5e5b38c18dd3                             |
  | internalurl  | http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s |
  | publicurl    | http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s |
  | region       | RegionOne                                                    |
  | service_id   | 37c4c0e79571404cb4644201a4a6e5ee                             |
  | service_name | swift                                                        |
  | service_type | object-store                                                 |
  +--------------+--------------------------------------------------------------+

Application Credentials and Access Rules
-----------------------------------------

OpenStack Keystone supports `Application Credentials`_, which allow users to
create tokens with a limited subset of their permissions. An application
credential may optionally carry *access rules* that restrict which HTTP methods
and URL paths are permitted for the ``object-store`` service.

RGW enforces these rules automatically. When validating a token, RGW sends the
``OpenStack-Identity-Access-Rules: 1.0`` request header to opt in to receiving
access rules from Keystone. If the validated token belongs to an application
credential that carries access rules, RGW checks every incoming request against
those rules and returns ``403 Forbidden`` if none of the rules match.

No additional configuration is required. Tokens that do not belong to an
application credential, or application credentials without an
``access_rules`` field, are unaffected. An application credential whose
``access_rules`` field is present but empty (``[]``) is a deliberate empty
whitelist: every request from such a token is denied with
``403 Forbidden``, matching the OpenStack ``keystonemiddleware`` behavior.

Access rule path patterns follow the `keystonemiddleware reference matcher`_:

* ``*`` matches one path segment (no ``/`` characters)
* ``**`` matches any number of path segments (including zero)
* ``{tag}`` matches one path segment (named placeholder)

For example, a rule with path ``/v1/AUTH_*/mycontainer/**`` permits requests
to any object inside ``mycontainer`` for any account, while ``/v1/*`` permits
requests only to the top-level account endpoint.

.. _Application Credentials: https://docs.openstack.org/keystone/latest/user/application_credentials.html
.. _keystonemiddleware reference matcher: https://opendev.org/openstack/keystonemiddleware/src/branch/master/keystonemiddleware/auth_token/__init__.py

Keystone Integration with the S3 API
------------------------------------

It is possible to use Keystone for authentication even when using the
S3 API (with AWS-like access and secret keys) if the
:confval:`rgw_s3_auth_use_keystone` option is set. For details, see
:doc:`s3/authentication`.

Requests authenticated via Keystone (Swift tokens or S3 with Keystone-managed
credentials) expose the Keystone identity in the IAM policy environment
as the condition keys: ``keystone:role`` (role names) and
``keystone:userid`` (user UUID). These conditions can be used in bucket
policies and idenitity policies with ``StringEquals``, ``StringNotEquals`` etc.

- **keystone:role** - Allow or deny by *role* (e.g. only users with role
  ``reader`` get read access). Use for RBAC.
- **keystone:userid** - Restrict to specific *user*. Use when policy
  depends on a specific user, not just their role.

See :doc:`bucketpolicy` for list of supported condition keys.

Service Token Support
---------------------

Service tokens can be enabled to support Ceph Object Gateway Keystone integration
to allow expired tokens when coupled with a valid service token in the request.

Enable the support with :confval:`rgw_keystone_service_token_enabled` and use the
:confval:`rgw_keystone_service_token_accepted_roles` option to specify which roles are considered
service roles.

The :confval:`rgw_keystone_expired_token_cache_expiration` option can be used to tune the cache
expiration for an expired token allowed with a service token. Please note that this must
be lower than the ``[token]/allow_expired_window`` option in the Keystone configuration.

Enabling this will cause an expired token given in the ``X-Auth-Token`` header to be allowed
if coupled with a ``X-Service-Token`` header that contains a valid token with the accepted
roles. This can allow long-running processes using a user token in ``X-Auth-Token`` to function
beyond the expiration of the token.
