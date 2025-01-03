=====================================
 Integrating with OpenStack Keystone
=====================================

It is possible to integrate the Ceph Object Gateway with Keystone, the OpenStack
identity service. This sets up the gateway to accept Keystone as the users
authority. A user that Keystone authorizes to access the gateway will also be
automatically created on the Ceph Object Gateway (if didn't exist beforehand). A
token that Keystone validates will be considered as valid by the gateway.

The following configuration options are available for Keystone integration::

	[client.radosgw.gateway]
	rgw keystone api version = {keystone api version}
	rgw keystone url = {keystone server url:keystone server admin port}
	rgw keystone admin token = {keystone admin token}
	rgw keystone admin token path = {path to keystone admin token} #preferred
	rgw keystone accepted roles = {accepted user roles}
	rgw keystone token cache size = {number of tokens to cache}
	rgw keystone implicit tenants = {true for private tenant for each new user}

It is also possible to configure a Keystone service tenant, user & password for
Keystone (for v2.0 version of the OpenStack Identity API), similar to the way
OpenStack services tend to be configured, this avoids the need for setting the
shared secret ``rgw keystone admin token`` in the configuration file, which is
recommended to be disabled in production environments. The service tenant
credentials should have admin privileges, for more details refer the `OpenStack
Keystone documentation`_, which explains the process in detail. The requisite
configuration options for are::

   rgw keystone admin user = {keystone service tenant user name}
   rgw keystone admin password = {keystone service tenant user password}
   rgw keystone admin password = {keystone service tenant user password path} # preferred
   rgw keystone admin tenant = {keystone service tenant name}


A Ceph Object Gateway user is mapped into a Keystone ``tenant``. A Keystone user
has different roles assigned to it on possibly more than a single tenant. When
the Ceph Object Gateway gets the ticket, it looks at the tenant, and the user
roles that are assigned to that ticket, and accepts/rejects the request
according to the ``rgw keystone accepted roles`` configurable.

For a v3 version of the OpenStack Identity API you should replace
``rgw keystone admin tenant`` with::

   rgw keystone admin domain = {keystone admin domain name}
   rgw keystone admin project = {keystone admin project name}

For compatibility with previous versions of ceph, it is also
possible to set ``rgw keystone implicit tenants`` to either
``s3`` or ``swift``.  This has the effect of splitting
the identity space such that the indicated protocol will
only use implicit tenants, and the other protocol will
never use implicit tenants.  Some older versions of ceph
only supported implicit tenants with swift.

Ocata (and later)
-----------------

Keystone itself needs to be configured to point to the Ceph Object Gateway as an
object-storage endpoint::

  openstack service create --name=swift \
                           --description="Swift Service" \
                           object-store
  +-------------+----------------------------------+
  | Field       | Value                            |
  +-------------+----------------------------------+
  | description | Swift Service                    |
  | enabled     | True                             |
  | id          | 37c4c0e79571404cb4644201a4a6e5ee |
  | name        | swift                            |
  | type        | object-store                     |
  +-------------+----------------------------------+

  openstack endpoint create --region RegionOne \
       --publicurl   "http://radosgw.example.com:8080/swift/v1" \
       --adminurl    "http://radosgw.example.com:8080/swift/v1" \
       --internalurl "http://radosgw.example.com:8080/swift/v1" \
       swift
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

  $ openstack endpoint show object-store
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

.. note:: If your radosgw ``ceph.conf`` sets the configuration option
	  ``rgw swift account in url = true``, your ``object-store``
	  endpoint URLs must be set to include the suffix
	  ``/v1/AUTH_%(tenant_id)s`` (instead of just ``/v1``).

The Keystone URL is the Keystone admin RESTful API URL. The admin token is the
token that is configured internally in Keystone for admin requests.

OpenStack Keystone may be terminated with a self signed ssl certificate, in
order for radosgw to interact with Keystone in such a case, you could either
install Keystone's ssl certificate in the node running radosgw. Alternatively
radosgw could be made to not verify the ssl certificate at all (similar to
OpenStack clients with a ``--insecure`` switch) by setting the value of the
configurable ``rgw keystone verify ssl`` to false.


.. _OpenStack Keystone documentation: http://docs.openstack.org/developer/keystone/configuringservices.html#setting-up-projects-users-and-roles

Cross Project(Tenant) Access
----------------------------

In order to let a project (earlier called a 'tenant') access buckets belonging to a different project, the following config option needs to be enabled::

   rgw swift account in url = true

The Keystone object-store endpoint must accordingly be configured to include the AUTH_%(project_id)s suffix::

   openstack endpoint create --region RegionOne \
       --publicurl   "http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s" \
       --adminurl    "http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s" \
       --internalurl "http://radosgw.example.com:8080/swift/v1/AUTH_$(project_id)s" \
       swift
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

Keystone integration with the S3 API
------------------------------------

It is possible to use Keystone for authentication even when using the
S3 API (with AWS-like access and secret keys), if the ``rgw s3 auth
use keystone`` option is set. For details, see
:doc:`s3/authentication`.

Service token support
---------------------

Service tokens can be enabled to support RadosGW Keystone integration
to allow expired tokens when coupled with a valid service token in the request.

Enable the support with ``rgw keystone service token enabled`` and use the
``rgw keystone service token accepted roles`` option to specify which roles are considered
service roles.

The ``rgw keystone expired token cache expiration`` option can be used to tune the cache
expiration for an expired token allowed with a service token, please note that this must
be lower than the ``[token]/allow_expired_window`` option in the Keystone configuration.

Enabling this will cause an expired token given in the X-Auth-Token header to be allowed
if coupled with a X-Service-Token header that contains a valid token with the accepted
roles. This can allow long running processes using a user token in X-Auth-Token to function
beyond the expiration of the token.
