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

.. warning::
   In legacy mode (the default), Keystone S3 authentication retrieves the
   user's raw root secret key over the network using
   ``GET /v3/users/<user>/OS-EC2/<credential>`` and caches it in memory.
   To avoid exposing raw root secrets to the gateway, you can configure
   the gateway to use scoped signing-key exchange instead. See
   `Scoped Signing-Key Authentication for S3`_ below.

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

Project-scoped reader role
--------------------------

By default any Keystone user accepted by the gateway is granted full control
over its project's object storage. ``rgw keystone accepted project reader
roles`` designates one or more Keystone roles as *project-scoped readers*
(the Swift ``project_reader_roles`` equivalent)::

   rgw keystone accepted roles = member
   rgw keystone accepted project reader roles = objectstore_viewer

When the reader role is the only access-granting role a user holds, the user
is capped to read-only within its Keystone project: it may read objects and
inspect bucket configuration (ACLs, policy, versioning, location, ...), but
not write. Every user of a project maps to the same RGW account (the project
itself), so a reader lists and reads every bucket the project owns through the
normal owner-scoped paths, and cannot reach another project (a different
owner). This scoping is by ownership and holds regardless of ``rgw keystone
implicit tenants``, which only controls the bucket-name namespace: with it
off, a reader reaching another project's bucket gets ``403`` (the bucket is
in the shared namespace, visible but not owned); with it on, it gets ``404``
(the bucket is in a separate namespace and not visible). Either way the
reader stays confined to its own project.

Note the following:

- **A project reader role admits the user on its own.** It need not also
  appear in ``rgw keystone accepted roles`` (mirroring Swift's
  ``project_reader_roles``): a user whose only granting role is a project
  reader role is admitted and capped to read-only.

- **The cap is a floor, not a hard ceiling.** A bucket policy that names the
  user (via a ``keystone:userid`` / ``keystone:role`` condition or a principal
  ARN) can still grant specific actions such as writes on specific buckets;
  an explicit policy ``Deny`` can likewise remove access. Only bucket creation
  and Swift account-metadata writes are refused up front regardless of policy.

- **A role that grants more than read overrides the cap.** If the same user
  also holds ``admin`` or any other role in ``rgw keystone accepted roles``
  (for example ``member``), that role wins and the user receives full control.
  To keep a user read-only, assign it *only* the reader role. Roles that are
  in no accepted list at all are ignored and do not affect the cap.

Implicit-deny roles
-------------------

``rgw keystone implicit deny roles`` designates Keystone roles
that authenticate a user without granting any implicit permissions::

   rgw keystone accepted roles = member
   rgw keystone implicit deny roles = objectstore_authenticated

A user whose only access-granting role is such a role is denied every
operation by default. Access must be granted explicitly through a bucket
policy or IAM policy (for example with a ``keystone:userid`` or
``keystone:role`` condition), and an explicit ``Allow`` then applies only to
the buckets and objects the policy names. Unlike a project reader, a user
with only an implicit-deny role cannot list the buckets of its project;
bucket creation and Swift account-metadata writes are always refused.

The project-reader notes above apply here as well: the role admits the user
on its own (no co-listing in ``rgw keystone accepted roles`` needed), an
explicit policy ``Deny`` always wins, and any role granting more (``admin``,
a project reader role, or another accepted role such as ``member``) takes
precedence over the implicit-deny cap.

.. note:: These tiers cap what a request may do at authorization time. They
   apply to the default Keystone mapping, where each project maps to a plain
   tenant-scoped gateway user reaching the gateway over S3 or Swift. They do
   **not** apply in the following cases:

   - **The librgw frontend (NFS-Ganesha).** It authorizes every request with
     full control, so a gateway reached over that path is not capped. Assign
     these roles only where clients use S3 or Swift.
   - **Keystone users associated with an** :doc:`RGW account <account>`.
     Access for an account is governed entirely by IAM policy (evaluated
     before the permission mask), so the tiers place no cap there. Use them
     with the default (non-account) mapping.
   - **The** ``radosgw-admin`` **command and the admin REST API.** These are
     backend administration paths that do not authenticate a Keystone user,
     so they are not subject to these roles at all.

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

Scoped Signing-Key Authentication for S3
----------------------------------------

By default (:confval:`rgw_keystone_s3_auth_mode` set to ``legacy``), the Ceph
Object Gateway authenticates S3 requests by looking up the client's raw root
secret key in Keystone via ``GET /v3/users/<user>/OS-EC2/<credential>`` and
caching that secret locally. This exposes users' permanent root secrets to the
gateway and over the network between Keystone and the gateway.

When :confval:`rgw_keystone_s3_auth_mode` is configured to ``signing_key``,
the gateway uses a cryptographic proof exchange protocol with Keystone:

- **AWS Signature Version 4 (SigV4)**: The gateway constructs a canonical
  string-to-sign from the client request and transmits an authenticated proof
  to Keystone at ``POST /v3/s3tokens/signing_key``. Keystone validates the
  request proof and returns a 32-byte binary derived signing key scoped strictly
  to the request's calendar date, region, and S3 service
  (``<date>/<region>/s3/aws4_request``). The gateway caches this derived key
  locally and computes the expected client signature. The user's permanent root
  secret is never retrieved, transmitted, or cached.
- **AWS Signature Version 2 (SigV2)**: When enabled via
  :confval:`rgw_keystone_s3_allow_legacy_signatures`, the gateway validates
  SigV2 requests on every invocation by forwarding the canonical proof to
  Keystone at ``POST /v3/s3tokens/validate``. Keystone performs identity-only
  verification; no signing key is returned and no positive caching is performed.

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

The following options configure Keystone scoped signing-key authentication:

.. confval:: rgw_keystone_s3_auth_mode
   :default: legacy
   :type: str

   Selects the Keystone S3 proof exchange protocol. Allowed values are:

   - ``legacy`` (default): Retrieves raw root secrets from Keystone.
   - ``signing_key``: Uses scoped signing-key exchange for SigV4 and per-request
     validation for SigV2. Never falls back to secret retrieval.

   Requires restarting the Ceph Object Gateway daemon after changing.

.. confval:: rgw_keystone_s3_allow_legacy_signatures
   :default: true
   :type: bool

   Controls whether deprecated AWS Signature Version 2 (SigV2) requests are
   permitted through Keystone. In ``signing_key`` mode, each SigV2 request
   requires fresh identity-only validation at ``POST /v3/s3tokens/validate``.
   This option does not disable independently configured local/RADOS user
   authentication. Note that this setting is independent of OpenStack Heat's
   legacy CloudFormation SigV2 options. Requires restarting the Ceph Object
   Gateway daemon after changing.

.. confval:: rgw_keystone_signing_key_cache_size
   :default: 1024
   :type: int

   Maximum number of scoped signing keys maintained in memory per Keystone S3
   engine context (plain and boto2 strategies each maintain their own cache
   bound). Valid range is ``0`` to ``10000``. Setting this value to ``0``
   disables cache storage (keys are not retained in memory), but does not
   disable authentication or timing verification checks. Cached entries are
   process-local only; keys are never persisted to disk, stored in memcached,
   or written to RADOS. Requires restarting the Ceph Object Gateway daemon after
   changing.

.. confval:: rgw_keystone_signing_key_cache_ttl
   :default: 300
   :type: int

   Maximum lifetime in seconds for a cached scoped signing key in local memory,
   and request-start monotonic admission ceiling. Valid range is ``0`` to
   ``3600``. When non-zero, this value defines both cache retention and a local
   monotonic admission limit measured from request initiation: if a slow
   network RPC or cold exchange takes longer than this configured interval
   (for example, a TTL of 1 second with a 2-second exchange), the request is
   denied at admission even if Keystone returned a future expiration. Setting
   this value to ``0`` disables cache storage without disabling authentication:
   each request undergoes live proof exchange, and admission remains bounded
   by the minimum of Keystone's ``cache_until``, identity expiration, and a
   hard 3600-second ceiling. Local cache eviction or expiration does not
   constitute cryptographic key revocation. Requires restarting the Ceph
   Object Gateway daemon after changing.

Required Rollout and Migration Order
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deploying scoped signing-key authentication requires a strict four-step rollout
order across the OpenStack and Ceph environments:

1. **Configure Keystone Server Policy and Service Credentials**:
   The Ceph Object Gateway authenticates to Keystone using its own service
   caller credentials (:confval:`rgw_keystone_admin_user`,
   :confval:`rgw_keystone_admin_password_path`). The service caller's role must
   be granted explicit policy permissions in Keystone's ``policy.yaml``:

   - ``identity:s3_signing_key``: Permits exchanging SigV4 proofs for derived
     signing keys at ``POST /v3/s3tokens/signing_key``.
   - ``identity:s3credentials_validate``: Permits validating SigV2 proofs at
     ``POST /v3/s3tokens/validate``.

   Keep Keystone in ``[credential] secret_read_mode = legacy`` during this phase
   so that existing raw-secret consumers continue to operate without interruption.

2. **Verify Capability with Authenticated OPTIONS and Signed Probe**:
   On initialization, the gateway issues an authenticated ``OPTIONS`` request
   to ``/v3/s3tokens/signing_key`` (and ``/v3/s3tokens/validate`` if SigV2 is
   enabled). Keystone must respond with HTTP ``2xx`` and an ``Allow`` header
   containing ``POST``.

   .. warning::
      An HTTP ``2xx`` response to an ``OPTIONS`` request verifies route presence
      and HTTP method support only. It does **not** verify that the gateway's
      service caller has been granted the required RBAC policies. Operators
      must execute an actual signed S3 probe request to confirm service policy
      authorization before directing production traffic to the new mode.

3. **Configure Ceph Object Gateway Instances**:
   Set :confval:`rgw_keystone_s3_auth_mode` to ``signing_key`` across gateway
   instances and perform a rolling restart while Keystone is still operating in
   legacy mode. Verify client compatibility across all expected traffic types:

   - SigV4 header-based authentication.
   - SigV4 query-string authentication (presigned URLs).
   - SigV4 streaming chunked uploads (``STREAMING-AWS4-HMAC-SHA256-PAYLOAD``).
   - SigV4 chunked uploads with trailing checksums and signatures
     (``STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER``).
   - SigV2 clients (such as boto2).

   After confirming that all legacy SigV2 clients have been upgraded, disable
   SigV2 by setting :confval:`rgw_keystone_s3_allow_legacy_signatures` to
   ``false``.

4. **Transition Keystone Server to Write-Only Mode**:
   Only after **all** Ceph Object Gateway instances and any other S3 consumers
   are confirmed compatible and operating in ``signing_key`` mode, transition
   Keystone to write-only mode by configuring ``keystone.conf``::

      [credential]
      secret_read_mode = write_only

   In ``write_only`` mode, Keystone metadata endpoints return metadata only,
   ``PATCH`` returns HTTP ``405 Method Not Allowed``, and ``GET`` requests for
   credential secrets are rejected. Credentials can only be created via
   ``POST``, which returns the secret key once upon creation.

   .. note::
      Existing untagged or mixed EC2/S3 credentials cannot export signing keys
      because Keystone requires ``allowed_consumers == ['s3']``. Users must
      recreate S3-only credentials and capture the secret upon creation. Once
      Keystone is in ``write_only`` mode, there is no root secret GET recovery.

Rollback Considerations
~~~~~~~~~~~~~~~~~~~~~~~

There is no automatic downgrade or protocol fallback. If a proof request is
rejected with HTTP ``403 Forbidden``, the gateway fails closed; it will not
attempt to retrieve a root secret or fall back to legacy endpoints.

If an operator must roll back the deployment, Keystone must be explicitly
reconfigured with ``[credential] secret_read_mode = legacy`` and restarted.
Rolling back Keystone re-exposes raw root secrets to read operations across the
deployment, which is a conscious security trade-off made by the operator.
The Ceph Object Gateway instances can then be reconfigured with
``rgw_keystone_s3_auth_mode = legacy`` and restarted.

Protocol Compatibility and Migration Limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Browser Form POST Policy Proofs are Unsupported (Fail-Closed)**:
  S3 Browser Form POST requests authenticated by HTML policy documents (both
  SigV2 and SigV4) are **unsupported** in Keystone ``signing_key`` mode. The
  Keystone proof exchange API requires a 4-line SigV4 string-to-sign or a
  canonical SigV2 proof; Keystone does not provide a policy JSON key exchange
  endpoint.

  When a browser policy request reaches a gateway configured with
  ``rgw_keystone_s3_auth_mode = signing_key``, the gateway fails closed and
  denies the request (HTTP ``403 Forbidden``). There is no fallback to root
  secret retrieval or bearer tokens.

  Deployments requiring S3 browser form POST uploads must either:

  1. Retain dedicated gateway instances configured with
     ``rgw_keystone_s3_auth_mode = legacy`` for browser-upload workloads, or
  2. Use local/RADOS user credentials (:confval:`rgw_s3_auth_use_keystone` set
     to ``false`` for those users or buckets), which continue to support
     browser form POST normally.

  This is a required migration limitation; not all S3 features are supported by
  the scoped signing-key exchange protocol.

- **Payload Streaming and Signed Trailers**:
  AWS SigV4 streaming chunked payloads and signed trailers
  (``STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER``) are fully supported. The
  gateway computes chunk HMAC and trailer HMAC signatures using the 32-byte
  derived signing key without requiring root secret derivation.

- **Boto2 S3 Signature Compatibility**:
  The gateway supports Boto2 S3 canonical header alternatives under the strict
  signing-key protocol. It does not fall back to unauthenticated or legacy paths
  if alternative canonicalization fails.

Process-Local Caching, Expiry, and Revocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **In-Memory Cache Structure and Capacity 0 Semantics**:
  Derived keys are stored in a process-local, thread-safe LRU cache. The cache
  key is a composite tuple of ``(access_key, date, region, service, endpoint)``.
  Each entry contains the 32-byte derived key, decoded identity
  (``TokenEnvelope``), credential ID, and expiration deadlines. Root secret
  material, client signatures, and request tokens are never cached.
  Setting :confval:`rgw_keystone_signing_key_cache_size` to ``0`` disables
  cache storage (no keys are stored in memory across requests); all request
  validation, timing, and admission checks remain fully enforced.

- **Request Validation on Every Request**:
  Even when a request hits a warm cached signing key, the gateway validates
  the client signature and request timing locally:

  - **Header-based SigV4**: The gateway validates the request timestamp and
    signature against the canonical request. In ``signing_key`` mode, the
    request timestamp must be no more than 300 seconds in the future (the
    existing legacy mode retains its 15-minute past/future window in
    the general request parser).
  - **Query-based SigV4 (Presigned URLs)**: The URL expiration interval
    (``X-Amz-Expires``) must be between 1 and 604,800 seconds (7 days), and the
    signed timestamp must be at most 300 seconds in the future.
  - **SigV2 Requests**: Verified on every request; no positive key cache is
    maintained.

- **Monotonic Admission Deadlines and TTL 0 Semantics**:
  The effective cache expiration deadline is computed from request initiation:

  .. code-block:: text

     local_policy_ttl = (rgw_keystone_signing_key_cache_ttl > 0) ? rgw_keystone_signing_key_cache_ttl : 3600
     effective_wall_deadline = min(keystone_cache_until, identity_expires_at, request_start_wall + local_policy_ttl)

  Monotonic deadlines are tracked simultaneously from request initiation
  (``request_start_mono + local_policy_ttl``) to guard against wall-clock skew,
  forward jumps, or rollback.

  When :confval:`rgw_keystone_signing_key_cache_ttl` is non-zero, it also serves
  as a request-start monotonic admission ceiling. If a slow network RPC or
  cold key exchange takes longer than the configured local TTL (for example,
  a TTL of 1 second with a 2-second exchange), the gateway denies the request
  at admission even if Keystone returned a future ``cache_until``.

  When :confval:`rgw_keystone_signing_key_cache_ttl` is set to ``0``, cache
  storage is disabled (no cached keys are retained or reused across requests),
  but admission verification is **not** skipped or immediately expired: admission
  remains bounded by ``min(keystone_cache_until, identity_expires_at, request_start_wall + 3600)``.

  .. warning::
     Evicting or purging a key from the gateway cache does **not**
     cryptographically revoke the derived signing key. A derived signing key
     has no intrinsic self-expiration; its date field is a domain-scoping
     string rather than a 24-hour self-invalidating clock. A party holding
     the derived key can compute HMAC signatures for that specific
     ``<date>/<region>/s3/aws4_request`` credential scope indefinitely; whether
     the request is accepted depends on verifier request-time validation,
     presigned URL expiration (which can legitimately extend up to 7 days
     beyond the signing date), and active credential policies. Cache deadline
     expiration is not cryptographic revocation.

     Revocation of users, projects, roles, or credentials in Keystone is
     reflected on the next uncached proof exchange with Keystone. Cached entries
     remain valid until their deadline expires or the daemon is restarted;
     instant revocation is not guaranteed. Setting
     :confval:`rgw_keystone_signing_key_cache_ttl` to ``0`` prevents the reuse
     of cached identities, proofs, and keys across requests by enforcing a live
     RPC exchange on every request; however, in-flight requests, network
     propagation, and remote policy timing still do not provide instant
     cryptographic revocation.

Network Security, Timeouts, and Redaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **TLS Verification**:
  Production deployments must keep :confval:`rgw_keystone_verify_ssl` set to
  ``true``. Keystone URLs must use HTTPS with valid certificates. The gateway
  never disables TLS verification after communication failures.

- **Bounded Timeouts**:
  Proof exchange requests enforce a 5-second connection timeout and a 10-second
  total request timeout.

- **Single 401 Token Refresh**:
  If Keystone returns HTTP ``401 Unauthorized`` for the gateway's service token,
  the gateway invalidates its cached service token, refetches a new service
  token, and retries the exact same proof endpoint once. If the second attempt
  returns 401, or if Keystone returns HTTP ``403 Forbidden``, the gateway stops
  and rejects the request. It does not loop, follow HTTP 3xx redirects, or fall
  back to legacy endpoints.

- **Logging and Redaction**:
  For the new proof exchange RPC and admin-token transport paths, the gateway
  avoids emitting client proof strings, canonical strings-to-sign, derived
  signing keys, service tokens, and administrator passwords into reachable
  diagnostic logs. Operators should nevertheless ensure that full-packet
  tracing, intermediate proxies, or external debugging tools do not capture
  sensitive request headers or tokens, as frontend logging across other
  subsystems is not globally guaranteed to be leak-free.

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

The following example illustrates a Ceph Object Gateway configuration for
scoped signing-key authentication in ``ceph.conf``::

   [client.radosgw.gateway]
   rgw_s3_auth_use_keystone = true
   rgw_keystone_api_version = 3
   rgw_keystone_url = https://keystone.example.com:5000
   rgw_keystone_verify_ssl = true
   rgw_keystone_admin_user = radosgw
   rgw_keystone_admin_password_path = /etc/ceph/keystone-admin-password.txt
   rgw_keystone_admin_domain = default
   rgw_keystone_admin_project = service
   rgw_keystone_accepted_roles = member, admin

   # Scoped signing-key configuration
   rgw_keystone_s3_auth_mode = signing_key
   rgw_keystone_s3_allow_legacy_signatures = true
   rgw_keystone_signing_key_cache_size = 1024
   rgw_keystone_signing_key_cache_ttl = 300

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
