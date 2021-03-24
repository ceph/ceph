======================================
 Ceph Object Gateway Config Reference
======================================

The following settings may added to the Ceph configuration file (i.e., usually
``ceph.conf``) under the ``[client.radosgw.{instance-name}]`` section. The
settings may contain default values. If you do not specify each setting in the
Ceph configuration file, the default value will be set automatically.

Configuration variables set under the ``[client.radosgw.{instance-name}]``
section will not apply to rgw or radosgw-admin commands without an instance-name
specified in the command. Thus variables meant to be applied to all RGW
instances or all radosgw-admin options can be put into the ``[global]`` or the
``[client]`` section to avoid specifying ``instance-name``.

``rgw_frontends``

:Description: Configures the HTTP frontend(s). The configuration for multiple
              frontends can be provided in a comma-delimited list. Each frontend
              configuration may include a list of options separated by spaces,
              where each option is in the form "key=value" or "key". See
              `HTTP Frontends`_ for more on supported options.

:Type: String
:Default: ``beast port=7480``

``rgw_data``

:Description: Sets the location of the data files for Ceph RADOS Gateway.
:Type: String
:Default: ``/var/lib/ceph/radosgw/$cluster-$id``


``rgw_enable_apis``

:Description: Enables the specified APIs.

              .. note:: Enabling the ``s3`` API is a requirement for
                        any ``radosgw`` instance that is meant to
                        participate in a `multi-site <../multisite>`_
                        configuration.
:Type: String
:Default: ``s3, s3website, swift, swift_auth, admin, sts, iam, notifications`` All APIs.


``rgw_cache_enabled``

:Description: Whether the Ceph Object Gateway cache is enabled.
:Type: Boolean
:Default: ``true``


``rgw_cache_lru_size``

:Description: The number of entries in the Ceph Object Gateway cache.
:Type: Integer
:Default: ``10000``


``rgw_socket_path``

:Description: The socket path for the domain socket. ``FastCgiExternalServer``
              uses this socket. If you do not specify a socket path, Ceph
              Object Gateway will not run as an external server. The path you
              specify here must be the same as the path specified in the
              ``rgw.conf`` file.

:Type: String
:Default: N/A

``rgw_fcgi_socket_backlog``

:Description: The socket backlog for fcgi.
:Type: Integer
:Default: ``1024``

``rgw_host``

:Description: The host for the Ceph Object Gateway instance. Can be an IP
              address or a hostname.

:Type: String
:Default: ``0.0.0.0``


``rgw_port``

:Description: Port the instance listens for requests. If not specified,
              Ceph Object Gateway runs external FastCGI.

:Type: String
:Default: None


``rgw_dns_name``

:Description: The DNS name of the served domain. See also the ``hostnames`` setting within regions.
:Type: String
:Default: None


``rgw_script_uri``

:Description: The alternative value for the ``SCRIPT_URI`` if not set
              in the request.

:Type: String
:Default: None


``rgw_request_uri``

:Description: The alternative value for the ``REQUEST_URI`` if not set
              in the request.

:Type: String
:Default: None


``rgw_print_continue``

:Description: Enable ``100-continue`` if it is operational.
:Type: Boolean
:Default: ``true``


``rgw_remote_addr_param``

:Description: The remote address parameter. For example, the HTTP field
              containing the remote address, or the ``X-Forwarded-For``
              address if a reverse proxy is operational.

:Type: String
:Default: ``REMOTE_ADDR``


``rgw_op_thread_timeout``

:Description: The timeout in seconds for open threads.
:Type: Integer
:Default: 600


``rgw_op_thread_suicide_timeout``

:Description: The time ``timeout`` in seconds before a Ceph Object Gateway
              process dies. Disabled if set to ``0``.

:Type: Integer
:Default: ``0``


``rgw_thread_pool_size``

:Description: The size of the thread pool.
:Type: Integer
:Default: 100 threads.


``rgw_num_control_oids``

:Description: The number of notification objects used for cache synchronization
              between different ``rgw`` instances.

:Type: Integer
:Default: ``8``


``rgw_init_timeout``

:Description: The number of seconds before Ceph Object Gateway gives up on
              initialization.

:Type: Integer
:Default: ``30``


``rgw_mime_types_file``

:Description: The path and location of the MIME-types file. Used for Swift
              auto-detection of object types.

:Type: String
:Default: ``/etc/mime.types``


``rgw_s3_success_create_obj_status``

:Description: The alternate success status response for ``create-obj``.
:Type: Integer
:Default: ``0``


``rgw_resolve_cname``

:Description: Whether ``rgw`` should use DNS CNAME record of the request
              hostname field (if hostname is not equal to ``rgw dns name``).

:Type: Boolean
:Default: ``false``


``rgw_obj_stripe_size``

:Description: The size of an object stripe for Ceph Object Gateway objects.
              See `Architecture`_ for details on striping.

:Type: Integer
:Default: ``4 << 20``


``rgw_extended_http_attrs``

:Description: Add new set of attributes that could be set on an entity
              (user, bucket or object). These extra attributes can be set
              through HTTP header fields when putting the entity or modifying
              it using POST method. If set, these attributes will return as
              HTTP  fields when doing GET/HEAD on the entity.

:Type: String
:Default: None
:Example: "content_foo, content_bar, x-foo-bar"


``rgw_exit_timeout_secs``

:Description: Number of seconds to wait for a process before exiting
              unconditionally.

:Type: Integer
:Default: ``120``


``rgw_get_obj_window_size``

:Description: The window size in bytes for a single object request.
:Type: Integer
:Default: ``16 << 20``


``rgw_get_obj_max_req_size``

:Description: The maximum request size of a single get operation sent to the
              Ceph Storage Cluster.

:Type: Integer
:Default: ``4 << 20``


``rgw_relaxed_s3_bucket_names``

:Description: Enables relaxed S3 bucket names rules for US region buckets.
:Type: Boolean
:Default: ``false``


``rgw_list_buckets_max_chunk``

:Description: The maximum number of buckets to retrieve in a single operation
              when listing user buckets.

:Type: Integer
:Default: ``1000``


``rgw_override_bucket_index_max_shards``

:Description: Represents the number of shards for the bucket index object,
              a value of zero indicates there is no sharding. It is not
              recommended to set a value too large (e.g. thousand) as it
              increases the cost for bucket listing.
              This variable should be set in the client or global sections
              so that it is automatically applied to radosgw-admin commands.

:Type: Integer
:Default: ``0``


``rgw_curl_wait_timeout_ms``

:Description: The timeout in milliseconds for certain ``curl`` calls.
:Type: Integer
:Default: ``1000``


``rgw_copy_obj_progress``

:Description: Enables output of object progress during long copy operations.
:Type: Boolean
:Default: ``true``


``rgw_copy_obj_progress_every_bytes``

:Description: The minimum bytes between copy progress output.
:Type: Integer
:Default: ``1024 * 1024``


``rgw_admin_entry``

:Description: The entry point for an admin request URL.
:Type: String
:Default: ``admin``


``rgw_content_length_compat``

:Description: Enable compatibility handling of FCGI requests with both ``CONTENT_LENGTH`` and ``HTTP_CONTENT_LENGTH`` set.
:Type: Boolean
:Default: ``false``


``rgw_bucket_quota_ttl``

:Description: The amount of time in seconds cached quota information is
              trusted.  After this timeout, the quota information will be
              re-fetched from the cluster.
:Type: Integer
:Default: ``600``


``rgw_user_quota_bucket_sync_interval``

:Description: The amount of time in seconds bucket quota information is
              accumulated before syncing to the cluster.  During this time,
              other RGW instances will not see the changes in bucket quota
              stats from operations on this instance.
:Type: Integer
:Default: ``180``


``rgw_user_quota_sync_interval``

:Description: The amount of time in seconds user quota information is
              accumulated before syncing to the cluster.  During this time,
              other RGW instances will not see the changes in user quota stats
              from operations on this instance.
:Type: Integer
:Default: ``180``


``rgw_bucket_default_quota_max_objects``

:Description: Default max number of objects per bucket. Set on new users,
              if no other quota is specified. Has no effect on existing users.
              This variable should be set in the client or global sections
              so that it is automatically applied to radosgw-admin commands.
:Type: Integer
:Default: ``-1``


``rgw_bucket_default_quota_max_size``

:Description: Default max capacity per bucket, in bytes. Set on new users,
              if no other quota is specified. Has no effect on existing users.
:Type: Integer
:Default: ``-1``


``rgw_user_default_quota_max_objects``

:Description: Default max number of objects for a user. This includes all
              objects in all buckets owned by the user. Set on new users,
              if no other quota is specified. Has no effect on existing users.
:Type: Integer
:Default: ``-1``


``rgw_user_default_quota_max_size``

:Description: The value for user max size quota in bytes set on new users,
              if no other quota is specified.  Has no effect on existing users.
:Type: Integer
:Default: ``-1``


``rgw_verify_ssl``

:Description: Verify SSL certificates while making requests.
:Type: Boolean
:Default: ``true``

Lifecycle Settings
==================

Bucket Lifecycle configuration can be used to manage your objects so they are stored
effectively throughout their lifetime. In past releases Lifecycle processing was rate-limited
by single threaded processing. With the Nautilus release this has been addressed and the
Ceph Object Gateway now allows for parallel thread processing of bucket lifecycles across
additional Ceph Object Gateway instances and replaces the in-order
index shard enumeration with a random ordered sequence.

There are two options in particular to look at when looking to increase the
aggressiveness of lifecycle processing:

``rgw_lc_max_worker``

:Description: This option specifies the number of lifecycle worker threads
              to run in parallel, thereby processing bucket and index
              shards simultaneously.

:Type: Integer
:Default: ``3``

``rgw_lc_max_wp_worker``

:Description: This option specifies the number of threads in each lifecycle
              workers work pool. This option can help accelerate processing each bucket.

These values can be tuned based upon your specific workload to further increase the
aggressiveness of lifecycle processing. For a workload with a larger number of buckets (thousands)
you would look at increasing the ``rgw_lc_max_worker`` value from the default value of 3 whereas for a
workload with a smaller number of buckets but higher number of objects (hundreds of thousands)
per bucket you would consider decreasing ``rgw_lc_max_wp_worker`` from the default value of 3.

:NOTE: When looking to tune either of these specific values please validate the
       current Cluster performance and Ceph Object Gateway utilization before increasing.

Garbage Collection Settings
===========================

The Ceph Object Gateway allocates storage for new objects immediately.

The Ceph Object Gateway purges the storage space used for deleted and overwritten 
objects in the Ceph Storage cluster some time after the gateway deletes the 
objects from the bucket index. The process of purging the deleted object data 
from the Ceph Storage cluster is known as Garbage Collection or GC.

To view the queue of objects awaiting garbage collection, execute the following::

  $ radosgw-admin gc list 

  Note: specify ``--include-all`` to list all entries, including unexpired
  
Garbage collection is a background activity that may
execute continuously or during times of low loads, depending upon how the
administrator configures the Ceph Object Gateway. By default, the Ceph Object
Gateway conducts GC operations continuously. Since GC operations are a normal
part of Ceph Object Gateway operations, especially with object delete
operations, objects eligible for garbage collection exist most of the time.

Some workloads may temporarily or permanently outpace the rate of garbage
collection activity. This is especially true of delete-heavy workloads, where
many objects get stored for a short period of time and then deleted. For these
types of workloads, administrators can increase the priority of garbage
collection operations relative to other operations with the following
configuration parameters.


``rgw_gc_max_objs``

:Description: The maximum number of objects that may be handled by
              garbage collection in one garbage collection processing cycle.
              Please do not change this value after the first deployment.

:Type: Integer
:Default: ``32``


``rgw_gc_obj_min_wait``

:Description: The minimum wait time before a deleted object may be removed
              and handled by garbage collection processing.

:Type: Integer
:Default: ``2 * 3600``


``rgw_gc_processor_max_time``

:Description: The maximum time between the beginning of two consecutive garbage
              collection processing cycles.

:Type: Integer
:Default: ``3600``


``rgw_gc_processor_period``

:Description: The cycle time for garbage collection processing.
:Type: Integer
:Default: ``3600``


``rgw_gc_max_concurrent_io``

:Description: The maximum number of concurrent IO operations that the RGW garbage
              collection thread will use when purging old data.
:Type: Integer
:Default: ``10``


:Tuning Garbage Collection for Delete Heavy Workloads:

As an initial step towards tuning Ceph Garbage Collection to be more aggressive the following options are suggested to be increased from their default configuration values:

``rgw_gc_max_concurrent_io = 20``
``rgw_gc_max_trim_chunk = 64``

:NOTE: Modifying these values requires a restart of the RGW service.

Once these values have been increased from default please monitor for performance of the cluster during Garbage Collection to verify no adverse performance issues due to the increased values.

Multisite Settings
==================

.. versionadded:: Jewel

You may include the following settings in your Ceph configuration
file under each ``[client.radosgw.{instance-name}]`` instance.


``rgw_zone``

:Description: The name of the zone for the gateway instance. If no zone is
              set, a cluster-wide default can be configured with the command
              ``radosgw-admin zone default``.
:Type: String
:Default: None


``rgw_zonegroup``

:Description: The name of the zonegroup for the gateway instance. If no
              zonegroup is set, a cluster-wide default can be configured with
              the command ``radosgw-admin zonegroup default``.
:Type: String
:Default: None


``rgw_realm``

:Description: The name of the realm for the gateway instance. If no realm is
              set, a cluster-wide default can be configured with the command
              ``radosgw-admin realm default``.
:Type: String
:Default: None


``rgw_run_sync_thread``

:Description: If there are other zones in the realm to sync from, spawn threads
              to handle the sync of data and metadata.
:Type: Boolean
:Default: ``true``


``rgw_data_log_window``

:Description: The data log entries window in seconds.
:Type: Integer
:Default: ``30``


``rgw_data_log_changes_size``

:Description: The number of in-memory entries to hold for the data changes log.
:Type: Integer
:Default: ``1000``


``rgw_data_log_obj_prefix``

:Description: The object name prefix for the data log.
:Type: String
:Default: ``data_log``


``rgw_data_log_num_shards``

:Description: The number of shards (objects) on which to keep the
              data changes log.

:Type: Integer
:Default: ``128``


``rgw_md_log_max_shards``

:Description: The maximum number of shards for the metadata log.
:Type: Integer
:Default: ``64``

.. important:: The values of ``rgw_data_log_num_shards`` and
   ``rgw_md_log_max_shards`` should not be changed after sync has
   started.

S3 Settings
===========

``rgw_s3_auth_use_ldap``

:Description: Should S3 authentication use LDAP?
:Type: Boolean
:Default: ``false``


Swift Settings
==============

``rgw_enforce_swift_acls``

:Description: Enforces the Swift Access Control List (ACL) settings.
:Type: Boolean
:Default: ``true``


``rgw_swift_token_expiration``

:Description: The time in seconds for expiring a Swift token.
:Type: Integer
:Default: ``24 * 3600``


``rgw_swift_url``

:Description: The URL for the Ceph Object Gateway Swift API.
:Type: String
:Default: None


``rgw_swift_url_prefix``

:Description: The URL prefix for the Swift API, to distinguish it from
              the S3 API endpoint. The default is ``swift``, which
              makes the Swift API available at the URL
              ``http://host:port/swift/v1`` (or
              ``http://host:port/swift/v1/AUTH_%(tenant_id)s`` if
              ``rgw swift account in url`` is enabled).

              For compatibility, setting this configuration variable
              to the empty string causes the default ``swift`` to be
              used; if you do want an empty prefix, set this option to
              ``/``.

              .. warning:: If you set this option to ``/``, you must
                           disable the S3 API by modifying ``rgw
                           enable apis`` to exclude ``s3``. It is not
                           possible to operate radosgw with ``rgw
                           swift url prefix = /`` and simultaneously
                           support both the S3 and Swift APIs. If you
                           do need to support both APIs without
                           prefixes, deploy multiple radosgw instances
                           to listen on different hosts (or ports)
                           instead, enabling some for S3 and some for
                           Swift.
:Default: ``swift``
:Example: "/swift-testing"


``rgw_swift_auth_url``

:Description: Default URL for verifying v1 auth tokens (if not using internal
              Swift auth).

:Type: String
:Default: None


``rgw_swift_auth_entry``

:Description: The entry point for a Swift auth URL.
:Type: String
:Default: ``auth``


``rgw_swift_account_in_url``

:Description: Whether or not the Swift account name should be included
              in the Swift API URL.

              If set to ``false`` (the default), then the Swift API
              will listen on a URL formed like
              ``http://host:port/<rgw_swift_url_prefix>/v1``, and the
              account name (commonly a Keystone project UUID if
              radosgw is configured with `Keystone integration
              <../keystone>`_) will be inferred from request
              headers.

              If set to ``true``, the Swift API URL will be
              ``http://host:port/<rgw_swift_url_prefix>/v1/AUTH_<account_name>``
              (or
              ``http://host:port/<rgw_swift_url_prefix>/v1/AUTH_<keystone_project_id>``)
              instead, and the Keystone ``object-store`` endpoint must
              accordingly be configured to include the
              ``AUTH_%(tenant_id)s`` suffix.

              You **must** set this option to ``true`` (and update the
              Keystone service catalog) if you want radosgw to support
              publicly-readable containers and `temporary URLs
              <../swift/tempurl>`_.
:Type: Boolean
:Default: ``false``


``rgw_swift_versioning_enabled``

:Description: Enables the Object Versioning of OpenStack Object Storage API.
              This allows clients to put the ``X-Versions-Location`` attribute
              on containers that should be versioned. The attribute specifies
              the name of container storing archived versions. It must be owned
              by the same user that the versioned container due to access
              control verification - ACLs are NOT taken into consideration.
              Those containers cannot be versioned by the S3 object versioning
              mechanism.

	      A slightly different attribute, ``X-History-Location``, which is also understood by
              `OpenStack Swift <https://docs.openstack.org/swift/latest/api/object_versioning.html>`_
              for handling ``DELETE`` operations, is currently not supported.
:Type: Boolean
:Default: ``false``


``rgw_trust_forwarded_https``

:Description: When a proxy in front of radosgw is used for ssl termination, radosgw
              does not know whether incoming http connections are secure. Enable
              this option to trust the ``Forwarded`` and ``X-Forwarded-Proto`` headers
              sent by the proxy when determining whether the connection is secure.
              This is required for some features, such as server side encryption.
              (Never enable this setting if you do not have a trusted proxy in front of
              radosgw, or else malicious users will be able to set these headers in
              any request.)
:Type: Boolean
:Default: ``false``



Logging Settings
================


``rgw_log_nonexistent_bucket``

:Description: Enables Ceph Object Gateway to log a request for a non-existent
              bucket.

:Type: Boolean
:Default: ``false``


``rgw_log_object_name``

:Description: The logging format for an object name. See ma npage
              :manpage:`date` for details about format specifiers.

:Type: Date
:Default: ``%Y-%m-%d-%H-%i-%n``


``rgw_log_object_name_utc``

:Description: Whether a logged object name includes a UTC time.
              If ``false``, it uses the local time.

:Type: Boolean
:Default: ``false``


``rgw_usage_max_shards``

:Description: The maximum number of shards for usage logging.
:Type: Integer
:Default: ``32``


``rgw_usage_max_user_shards``

:Description: The maximum number of shards used for a single user's
              usage logging.

:Type: Integer
:Default: ``1``


``rgw_enable_ops_log``

:Description: Enable logging for each successful Ceph Object Gateway operation.
:Type: Boolean
:Default: ``false``


``rgw_enable_usage_log``

:Description: Enable the usage log.
:Type: Boolean
:Default: ``false``


``rgw_ops_log_rados``

:Description: Whether the operations log should be written to the
              Ceph Storage Cluster backend.

:Type: Boolean
:Default: ``true``


``rgw_ops_log_socket_path``

:Description: The Unix domain socket for writing operations logs.
:Type: String
:Default: None


``rgw_ops_log_data_backlog``

:Description: The maximum data backlog data size for operations logs written
              to a Unix domain socket.

:Type: Integer
:Default: ``5 << 20``


``rgw_usage_log_flush_threshold``

:Description: The number of dirty merged entries in the usage log before
              flushing synchronously.

:Type: Integer
:Default: 1024


``rgw_usage_log_tick_interval``

:Description: Flush pending usage log data every ``n`` seconds.
:Type: Integer
:Default: ``30``


``rgw_log_http_headers``

:Description: Comma-delimited list of HTTP headers to include with ops
	      log entries.  Header names are case insensitive, and use
	      the full header name with words separated by underscores.

:Type: String
:Default: None
:Example: "http_x_forwarded_for, http_x_special_k"


``rgw_intent_log_object_name``

:Description: The logging format for the intent log object name. See the manpage
              :manpage:`date` for details about format specifiers.

:Type: Date
:Default: ``%Y-%m-%d-%i-%n``


``rgw_intent_log_object_name_utc``

:Description: Whether the intent log object name uses UTC time.
              If ``false``, it uses the local time.

:Type: Boolean
:Default: ``false``



Keystone Settings
=================


``rgw_keystone_url``

:Description: The URL for the Keystone server.
:Type: String
:Default: None


``rgw_keystone_api_version``

:Description: The version (2 or 3) of OpenStack Identity API that should be
              used for communication with the Keystone server.
:Type: Integer
:Default: ``2``


``rgw_keystone_admin_domain``

:Description: The name of OpenStack domain with admin privilege when using
              OpenStack Identity API v3.
:Type: String
:Default: None


``rgw_keystone_admin_project``

:Description: The name of OpenStack project with admin privilege when using
              OpenStack Identity API v3. If left unspecified, value of
              ``rgw keystone admin tenant`` will be used instead.
:Type: String
:Default: None


``rgw_keystone_admin_token``

:Description: The Keystone admin token (shared secret). In Ceph RGW
              authentication with the admin token has priority over
              authentication with the admin credentials
              (``rgw_keystone_admin_user``, ``rgw_keystone_admin_password``,
              ``rgw_keystone_admin_tenant``, ``rgw_keystone_admin_project``,
              ``rgw_keystone_admin_domain``). The Keystone admin token
              has been deprecated, but can be used to integrate with
              older environments.  It is preferred to instead configure
              ``rgw_keystone_admin_token_path`` to avoid exposing the token.
:Type: String
:Default: None

``rgw_keystone_admin_token_path``

:Description: Path to a file containing the Keystone admin token
	      (shared secret).  In Ceph RadosGW authentication with
	      the admin token has priority over authentication with
	      the admin credentials
              (``rgw_keystone_admin_user``, ``rgw_keystone_admin_password``,
              ``rgw_keystone_admin_tenant``, ``rgw_keystone_admin_project``,
              ``rgw_keystone_admin_domain``).
              The Keystone admin token has been deprecated, but can be
              used to integrate with older environments.
:Type: String
:Default: None

``rgw_keystone_admin_tenant``

:Description: The name of OpenStack tenant with admin privilege (Service Tenant) when
              using OpenStack Identity API v2
:Type: String
:Default: None


``rgw_keystone_admin_user``

:Description: The name of OpenStack user with admin privilege for Keystone
              authentication (Service User) when using OpenStack Identity API v2
:Type: String
:Default: None


``rgw_keystone_admin_password``

:Description: The password for OpenStack admin user when using OpenStack
              Identity API v2.  It is preferred to instead configure
              ``rgw_keystone_admin_password_path`` to avoid exposing the token.
:Type: String
:Default: None

``rgw_keystone_admin_password_path``

:Description: Path to a file containing the password for OpenStack
              admin user when using OpenStack Identity API v2.
:Type: String
:Default: None


``rgw_keystone_accepted_roles``

:Description: The roles required to serve requests.
:Type: String
:Default: ``Member, admin``


``rgw_keystone_token_cache_size``

:Description: The maximum number of entries in each Keystone token cache.
:Type: Integer
:Default: ``10000``


``rgw_keystone_revocation_interval``

:Description: The number of seconds between token revocation checks.
:Type: Integer
:Default: ``15 * 60``


``rgw_keystone_verify_ssl``

:Description: Verify SSL certificates while making token requests to keystone.
:Type: Boolean
:Default: ``true``


Server-side encryption Settings
===============================

``rgw_crypt_s3_kms_backend``

:Description: Where the SSE-KMS encryption keys are stored. Supported KMS
              systems are OpenStack Barbican (``barbican``, the default) and
              HashiCorp Vault (``vault``).
:Type: String
:Default: None


Barbican Settings
=================

``rgw_barbican_url``

:Description: The URL for the Barbican server.
:Type: String
:Default: None

``rgw_keystone_barbican_user``

:Description: The name of the OpenStack user with access to the `Barbican`_
              secrets used for `Encryption`_.
:Type: String
:Default: None

``rgw_keystone_barbican_password``

:Description: The password associated with the `Barbican`_ user.
:Type: String
:Default: None

``rgw_keystone_barbican_tenant``

:Description: The name of the OpenStack tenant associated with the `Barbican`_
              user when using OpenStack Identity API v2.
:Type: String
:Default: None

``rgw_keystone_barbican_project``

:Description: The name of the OpenStack project associated with the `Barbican`_
              user when using OpenStack Identity API v3.
:Type: String
:Default: None

``rgw_keystone_barbican_domain``

:Description: The name of the OpenStack domain associated with the `Barbican`_
              user when using OpenStack Identity API v3.
:Type: String
:Default: None


HashiCorp Vault Settings
========================

``rgw_crypt_vault_auth``

:Description: Type of authentication method to be used. The only method
              currently supported is ``token``.
:Type: String
:Default: ``token``

``rgw_crypt_vault_token_file``

:Description: If authentication method is ``token``, provide a path to the token
              file, which should be readable only by Rados Gateway.
:Type: String
:Default: None

``rgw_crypt_vault_addr``

:Description: Vault server base address, e.g. ``http://vaultserver:8200``.
:Type: String
:Default: None

``rgw_crypt_vault_prefix``

:Description: The Vault secret URL prefix, which can be used to restrict access
              to a particular subset of the secret space, e.g. ``/v1/secret/data``.
:Type: String
:Default: None

``rgw_crypt_vault_secret_engine``

:Description: Vault Secret Engine to be used to retrieve encryption keys: choose
              between kv-v2, transit.
:Type: String
:Default: None

``rgw_crypt_vault_namespace``

:Description: If set, Vault Namespace provides tenant isolation for teams and individuals
              on the same Vault Enterprise instance, e.g. ``acme/tenant1``
:Type: String
:Default: None


QoS settings
------------

.. versionadded:: Nautilus

The ``civetweb`` frontend has a threading model that uses a thread per
connection and hence is automatically throttled by ``rgw_thread_pool_size``
configurable when it comes to accepting connections. The newer ``beast`` frontend is
not restricted by the thread pool size when it comes to accepting new
connections, so a scheduler abstraction is introduced in the Nautilus release
to support future methods of scheduling requests.

Currently the scheduler defaults to a throttler which throttles the active
connections to a configured limit. QoS based on mClock is currently in an
*experimental* phase and not recommended for production yet. Current
implementation of *dmclock_client* op queue divides RGW Ops on admin, auth
(swift auth, sts) metadata & data requests.


``rgw_max_concurrent_requests``

:Description: Maximum number of concurrent HTTP requests that the Beast front end
              will process. Tuning this can help to limit memory usage under
              heavy load.
:Type: Integer
:Default: 1024


``rgw_scheduler_type``

:Description: The RGW scheduler to use. Valid values are ``throttler` and
              ``dmclock``. Currently defaults to ``throttler`` which throttles Beast
              frontend requests. ``dmclock` is *experimental* and requires the
              ``dmclock`` to be included in the ``experimental_feature_enabled``
              configuration option.


The options below tune the experimental dmclock scheduler. For
additional reading on dmclock, see :ref:`dmclock-qos`. `op_class` for the flags below is
one of ``admin``, ``auth``, ``metadata``, or ``data``.

``rgw_dmclock_<op_class>_res``

:Description: The mclock reservation for `op_class` requests
:Type: float
:Default: 100.0

``rgw_dmclock_<op_class>_wgt``

:Description: The mclock weight for `op_class` requests
:Type: float
:Default: 1.0

``rgw_dmclock_<op_class>_lim``

:Description: The mclock limit for `op_class` requests
:Type: float
:Default: 0.0



.. _Architecture: ../../architecture#data-striping
.. _Pool Configuration: ../../rados/configuration/pool-pg-config-ref/
.. _Cluster Pools: ../../rados/operations/pools
.. _Rados cluster handles: ../../rados/api/librados-intro/#step-2-configuring-a-cluster-handle
.. _Barbican: ../barbican
.. _Encryption: ../encryption
.. _HTTP Frontends: ../frontends
