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
instances or all radosgw-admin commands can be put into the ``[global]`` or the
``[client]`` section to avoid specifying instance-name.

``rgw frontends``

:Description: Configures the HTTP frontend(s). The configuration for multiple
              frontends can be provided in a comma-delimited list. Each frontend
              configuration may include a list of options separated by spaces,
              where each option is in the form "key=value" or "key". See
              `HTTP Frontends`_ for more on supported options.

:Type: String
:Default: ``beast port=7480``

``rgw data``

:Description: Sets the location of the data files for Ceph Object Gateway.
:Type: String
:Default: ``/var/lib/ceph/radosgw/$cluster-$id``


``rgw enable apis``

:Description: Enables the specified APIs.

              .. note:: Enabling the ``s3`` API is a requirement for
                        any radosgw instance that is meant to
                        participate in a `multi-site <../multisite>`_
                        configuration.
:Type: String
:Default: ``s3, swift, swift_auth, admin`` All APIs.


``rgw cache enabled``

:Description: Whether the Ceph Object Gateway cache is enabled.
:Type: Boolean
:Default: ``true``


``rgw cache lru size``

:Description: The number of entries in the Ceph Object Gateway cache.
:Type: Integer
:Default: ``10000``
	

``rgw socket path``

:Description: The socket path for the domain socket. ``FastCgiExternalServer`` 
              uses this socket. If you do not specify a socket path, Ceph 
              Object Gateway will not run as an external server. The path you 
              specify here must be the same as the path specified in the 
              ``rgw.conf`` file.

:Type: String
:Default: N/A

``rgw fcgi socket backlog``

:Description: The socket backlog for fcgi.
:Type: Integer
:Default: ``1024``

``rgw host``

:Description: The host for the Ceph Object Gateway instance. Can be an IP 
              address or a hostname.

:Type: String
:Default: ``0.0.0.0``


``rgw port``

:Description: Port the instance listens for requests. If not specified, 
              Ceph Object Gateway runs external FastCGI.
              
:Type: String
:Default: None


``rgw dns name``

:Description: The DNS name of the served domain. See also the ``hostnames`` setting within regions.
:Type: String 
:Default: None
	

``rgw script uri``

:Description: The alternative value for the ``SCRIPT_URI`` if not set
              in the request.

:Type: String
:Default: None


``rgw request uri``

:Description: The alternative value for the ``REQUEST_URI`` if not set
              in the request.

:Type: String
:Default: None


``rgw print continue``

:Description: Enable ``100-continue`` if it is operational.
:Type: Boolean
:Default: ``true``


``rgw remote addr param``

:Description: The remote address parameter. For example, the HTTP field 
              containing the remote address, or the ``X-Forwarded-For`` 
              address if a reverse proxy is operational.

:Type: String
:Default: ``REMOTE_ADDR``


``rgw op thread timeout``
	
:Description: The timeout in seconds for open threads.
:Type: Integer
:Default: 600
	

``rgw op thread suicide timeout``
	
:Description: The time ``timeout`` in seconds before a Ceph Object Gateway 
              process dies. Disabled if set to ``0``.

:Type: Integer 
:Default: ``0``


``rgw thread pool size``

:Description: The size of the thread pool.
:Type: Integer 
:Default: 100 threads.


``rgw num control oids``

:Description: The number of notification objects used for cache synchronization
              between different ``rgw`` instances.

:Type: Integer
:Default: ``8``


``rgw init timeout``

:Description: The number of seconds before Ceph Object Gateway gives up on 
              initialization.

:Type: Integer
:Default: ``30``


``rgw mime types file``

:Description: The path and location of the MIME types. Used for Swift 
              auto-detection of object types.

:Type: String
:Default: ``/etc/mime.types``


``rgw gc max objs``

:Description: The maximum number of objects that may be handled by 
              garbage collection in one garbage collection processing cycle.

:Type: Integer
:Default: ``32``


``rgw gc obj min wait``

:Description: The minimum wait time before the object may be removed 
              and handled by garbage collection processing.
              
:Type: Integer
:Default: ``2 * 3600``


``rgw gc processor max time``

:Description: The maximum time between the beginning of two consecutive garbage 
              collection processing cycles.

:Type: Integer
:Default: ``3600``


``rgw gc processor period``

:Description: The cycle time for garbage collection processing.
:Type: Integer
:Default: ``3600``


``rgw s3 success create obj status``

:Description: The alternate success status response for ``create-obj``.
:Type: Integer
:Default: ``0``


``rgw resolve cname``

:Description: Whether ``rgw`` should use DNS CNAME record of the request 
              hostname field (if hostname is not equal to ``rgw dns name``).

:Type: Boolean
:Default: ``false``


``rgw obj stripe size``

:Description: The size of an object stripe for Ceph Object Gateway objects.
              See `Architecture`_ for details on striping.

:Type: Integer
:Default: ``4 << 20``


``rgw extended http attrs``

:Description: Add new set of attributes that could be set on an entity
              (user, bucket or object). These extra attributes can be set
              through HTTP header fields when putting the entity or modifying
              it using POST method. If set, these attributes will return as
              HTTP  fields when doing GET/HEAD on the entity.

:Type: String
:Default: None
:Example: "content_foo, content_bar, x-foo-bar"


``rgw exit timeout secs``

:Description: Number of seconds to wait for a process before exiting 
              unconditionally.

:Type: Integer
:Default: ``120``


``rgw get obj window size``

:Description: The window size in bytes for a single object request.
:Type: Integer
:Default: ``16 << 20``


``rgw get obj max req size``

:Description: The maximum request size of a single get operation sent to the
              Ceph Storage Cluster.

:Type: Integer
:Default: ``4 << 20``

 
``rgw relaxed s3 bucket names``

:Description: Enables relaxed S3 bucket names rules for US region buckets.
:Type: Boolean
:Default: ``false``


``rgw list buckets max chunk``

:Description: The maximum number of buckets to retrieve in a single operation
              when listing user buckets.

:Type: Integer
:Default: ``1000``


``rgw override bucket index max shards``

:Description: Represents the number of shards for the bucket index object,
              a value of zero indicates there is no sharding. It is not
              recommended to set a value too large (e.g. thousand) as it
              increases the cost for bucket listing.
              This variable should be set in the client or global sections
              so that it is automatically applied to radosgw-admin commands.

:Type: Integer
:Default: ``0``


``rgw curl wait timeout ms``

:Description: The timeout in milliseconds for certain ``curl`` calls. 
:Type: Integer
:Default: ``1000``


``rgw copy obj progress``

:Description: Enables output of object progress during long copy operations.
:Type: Boolean
:Default: ``true``


``rgw copy obj progress every bytes``

:Description: The minimum bytes between copy progress output.
:Type: Integer 
:Default: ``1024 * 1024``


``rgw admin entry``

:Description: The entry point for an admin request URL.
:Type: String
:Default: ``admin``


``rgw content length compat``

:Description: Enable compatibility handling of FCGI requests with both CONTENT_LENGTH AND HTTP_CONTENT_LENGTH set.
:Type: Boolean
:Default: ``false``


``rgw bucket quota ttl``

:Description: The amount of time in seconds cached quota information is
              trusted.  After this timeout, the quota information will be
              re-fetched from the cluster.
:Type: Integer
:Default: ``600``


``rgw user quota bucket sync interval``

:Description: The amount of time in seconds bucket quota information is
              accumulated before syncing to the cluster.  During this time,
              other RGW instances will not see the changes in bucket quota
              stats from operations on this instance.
:Type: Integer
:Default: ``180``


``rgw user quota sync interval``

:Description: The amount of time in seconds user quota information is
              accumulated before syncing to the cluster.  During this time,
              other RGW instances will not see the changes in user quota stats
              from operations on this instance.
:Type: Integer
:Default: ``180``


``rgw bucket default quota max objects``

:Description: Default max number of objects per bucket. Set on new users,
              if no other quota is specified. Has no effect on existing users.
              This variable should be set in the client or global sections
              so that it is automatically applied to radosgw-admin commands.
:Type: Integer
:Default: ``-1``


``rgw bucket default quota max size``

:Description: Default max capacity per bucket, in bytes. Set on new users,
              if no other quota is specified. Has no effect on existing users.
:Type: Integer
:Default: ``-1``


``rgw user default quota max objects``

:Description: Default max number of objects for a user. This includes all
              objects in all buckets owned by the user. Set on new users,
              if no other quota is specified. Has no effect on existing users.
:Type: Integer
:Default: ``-1``


``rgw user default quota max size``

:Description: The value for user max size quota in bytes set on new users,
              if no other quota is specified.  Has no effect on existing users.
:Type: Integer
:Default: ``-1``


``rgw verify ssl``

:Description: Verify SSL certificates while making requests.
:Type: Boolean
:Default: ``true``


Multisite Settings
==================

.. versionadded:: Jewel

You may include the following settings in your Ceph configuration
file under each ``[client.radosgw.{instance-name}]`` instance.


``rgw zone``

:Description: The name of the zone for the gateway instance. If no zone is
              set, a cluster-wide default can be configured with the command
              ``radosgw-admin zone default``.
:Type: String
:Default: None


``rgw zonegroup``

:Description: The name of the zonegroup for the gateway instance. If no
              zonegroup is set, a cluster-wide default can be configured with
              the command ``radosgw-admin zonegroup default``.
:Type: String
:Default: None


``rgw realm``

:Description: The name of the realm for the gateway instance. If no realm is
              set, a cluster-wide default can be configured with the command
              ``radosgw-admin realm default``.
:Type: String
:Default: None


``rgw run sync thread``

:Description: If there are other zones in the realm to sync from, spawn threads
              to handle the sync of data and metadata.
:Type: Boolean
:Default: ``true``


``rgw data log window``

:Description: The data log entries window in seconds.
:Type: Integer
:Default: ``30``


``rgw data log changes size``

:Description: The number of in-memory entries to hold for the data changes log.
:Type: Integer
:Default: ``1000``


``rgw data log obj prefix``

:Description: The object name prefix for the data log.
:Type: String
:Default: ``data_log``


``rgw data log num shards``

:Description: The number of shards (objects) on which to keep the
              data changes log.

:Type: Integer
:Default: ``128``


``rgw md log max shards``

:Description: The maximum number of shards for the metadata log.
:Type: Integer
:Default: ``64``

.. important:: The values of ``rgw data log num shards`` and
   ``rgw md log max shards`` should not be changed after sync has
   started.


Swift Settings
==============

``rgw enforce swift acls``

:Description: Enforces the Swift Access Control List (ACL) settings.
:Type: Boolean
:Default: ``true``
	
	
``rgw swift token expiration``

:Description: The time in seconds for expiring a Swift token.
:Type: Integer
:Default: ``24 * 3600``


``rgw swift url``

:Description: The URL for the Ceph Object Gateway Swift API.
:Type: String
:Default: None
	

``rgw swift url prefix``

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


``rgw swift auth url``

:Description: Default URL for verifying v1 auth tokens (if not using internal 
              Swift auth).

:Type: String
:Default: None


``rgw swift auth entry``

:Description: The entry point for a Swift auth URL.
:Type: String
:Default: ``auth``


``rgw swift account in url``

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


``rgw swift versioning enabled``

:Description: Enables the Object Versioning of OpenStack Object Storage API.
              This allows clients to put the ``X-Versions-Location`` attribute
              on containers that should be versioned. The attribute specifies
              the name of container storing archived versions. It must be owned
              by the same user that the versioned container due to access
              control verification - ACLs are NOT taken into consideration.
              Those containers cannot be versioned by the S3 object versioning
              mechanism.

	      The ``X-History-Location`` attribute, also understood by
	      OpenStack Swift for handling ``DELETE`` operations
	      `slightly differently
	      <https://docs.openstack.org/swift/latest/overview_object_versioning.html>`_
	      from ``X-Versions-Location``, is currently not
	      supported.
:Type: Boolean
:Default: ``false``


``rgw trust forwarded https``

:Description: When a proxy in front of radosgw is used for ssl termination, radosgw
              does not know whether incoming http connections are secure. Enable
              this option to trust the ``Forwarded`` and ``X-Forwarded-Proto`` headers
              sent by the proxy when determining whether the connection is secure.
              This is required for some features, such as server side encryption.
:Type: Boolean
:Default: ``false``



Logging Settings
================


``rgw log nonexistent bucket``

:Description: Enables Ceph Object Gateway to log a request for a non-existent 
              bucket.

:Type: Boolean
:Default: ``false``


``rgw log object name``

:Description: The logging format for an object name. See manpage 
              :manpage:`date` for details about format specifiers.

:Type: Date
:Default: ``%Y-%m-%d-%H-%i-%n``


``rgw log object name utc``

:Description: Whether a logged object name includes a UTC time. 
              If ``false``, it uses the local time.

:Type: Boolean
:Default: ``false``


``rgw usage max shards``

:Description: The maximum number of shards for usage logging.
:Type: Integer
:Default: ``32``


``rgw usage max user shards``

:Description: The maximum number of shards used for a single user's 
              usage logging.

:Type: Integer
:Default: ``1``


``rgw enable ops log``

:Description: Enable logging for each successful Ceph Object Gateway operation.
:Type: Boolean
:Default: ``false``


``rgw enable usage log``

:Description: Enable the usage log.
:Type: Boolean
:Default: ``false``


``rgw ops log rados``

:Description: Whether the operations log should be written to the 
              Ceph Storage Cluster backend.

:Type: Boolean
:Default: ``true``


``rgw ops log socket path``

:Description: The Unix domain socket for writing operations logs.
:Type: String
:Default: None


``rgw ops log data backlog``

:Description: The maximum data backlog data size for operations logs written
              to a Unix domain socket.

:Type: Integer
:Default: ``5 << 20``


``rgw usage log flush threshold``

:Description: The number of dirty merged entries in the usage log before 
              flushing synchronously.

:Type: Integer
:Default: 1024


``rgw usage log tick interval``

:Description: Flush pending usage log data every ``n`` seconds.
:Type: Integer
:Default: ``30``


``rgw log http headers``

:Description: Comma-delimited list of HTTP headers to include with ops
	      log entries.  Header names are case insensitive, and use
	      the full header name with words separated by underscores.

:Type: String
:Default: None
:Example: "http_x_forwarded_for, http_x_special_k"


``rgw intent log object name``

:Description: The logging format for the intent log object name. See manpage 
              :manpage:`date` for details about format specifiers.

:Type: Date
:Default: ``%Y-%m-%d-%i-%n``


``rgw intent log object name utc``

:Description: Whether the intent log object name includes a UTC time. 
              If ``false``, it uses the local time.

:Type: Boolean
:Default: ``false``



Keystone Settings
=================


``rgw keystone url``

:Description: The URL for the Keystone server.
:Type: String
:Default: None


``rgw keystone api version``

:Description: The version (2 or 3) of OpenStack Identity API that should be
              used for communication with the Keystone server.
:Type: Integer
:Default: ``2``


``rgw keystone admin domain``

:Description: The name of OpenStack domain with admin privilege when using
              OpenStack Identity API v3.
:Type: String
:Default: None


``rgw keystone admin project``

:Description: The name of OpenStack project with admin privilege when using
              OpenStack Identity API v3. If left unspecified, value of
              ``rgw keystone admin tenant`` will be used instead.
:Type: String
:Default: None


``rgw keystone admin token``

:Description: The Keystone admin token (shared secret). In Ceph RadosGW
              authentication with the admin token has priority over
              authentication with the admin credentials
              (``rgw keystone admin user``, ``rgw keystone admin password``,
              ``rgw keystone admin tenant``, ``rgw keystone admin project``,
              ``rgw keystone admin domain``). The Keystone admin token
              has been deprecated, but can be used to integrate with
              older environments.  Prefer ``rgw keystone admin token path``
              to avoid exposing the token.
:Type: String
:Default: None

``rgw keystone admin token path``

:Description: Path to a file containing the Keystone admin token
	      (shared secret).  In Ceph RadosGW authentication with
	      the admin token has priority over authentication with
	      the admin credentials
              (``rgw keystone admin user``, ``rgw keystone admin password``,
              ``rgw keystone admin tenant``, ``rgw keystone admin project``,
              ``rgw keystone admin domain``).
              The Keystone admin token has been deprecated, but can be
              used to integrate with older environments.
:Type: String
:Default: None

``rgw keystone admin tenant``

:Description: The name of OpenStack tenant with admin privilege (Service Tenant) when
              using OpenStack Identity API v2
:Type: String
:Default: None


``rgw keystone admin user``

:Description: The name of OpenStack user with admin privilege for Keystone
              authentication (Service User) when OpenStack Identity API v2
:Type: String
:Default: None


``rgw keystone admin password``

:Description: The password for OpenStack admin user when using OpenStack
              Identity API v2.  Prefer ``rgw keystone admin password path``
              to avoid exposing the token.
:Type: String
:Default: None

``rgw keystone admin password path``

:Description: Path to a file containing the password for OpenStack
              admin user when using OpenStack Identity API v2.
:Type: String
:Default: None


``rgw keystone accepted roles``

:Description: The roles requires to serve requests.
:Type: String
:Default: ``Member, admin``


``rgw keystone token cache size``

:Description: The maximum number of entries in each Keystone token cache.
:Type: Integer
:Default: ``10000``


``rgw keystone revocation interval``

:Description: The number of seconds between token revocation checks.
:Type: Integer
:Default: ``15 * 60``


``rgw keystone verify ssl``

:Description: Verify SSL certificates while making token requests to keystone.
:Type: Boolean
:Default: ``true``

Barbican Settings
=================

``rgw barbican url``

:Description: The URL for the Barbican server.
:Type: String
:Default: None

``rgw keystone barbican user``

:Description: The name of the OpenStack user with access to the `Barbican`_
              secrets used for `Encryption`_.
:Type: String
:Default: None

``rgw keystone barbican password``

:Description: The password associated with the `Barbican`_ user.
:Type: String
:Default: None

``rgw keystone barbican tenant``

:Description: The name of the OpenStack tenant associated with the `Barbican`_
              user when using OpenStack Identity API v2.
:Type: String
:Default: None

``rgw keystone barbican project``

:Description: The name of the OpenStack project associated with the `Barbican`_
              user when using OpenStack Identity API v3.
:Type: String
:Default: None

``rgw keystone barbican domain``

:Description: The name of the OpenStack domain associated with the `Barbican`_
              user when using OpenStack Identity API v3.
:Type: String
:Default: None


QoS settings
------------

.. versionadded:: Nautilus

The ``civetweb`` frontend has a threading model that uses a thread per
connection and hence automatically throttled by ``rgw thread pool size``
configurable when it comes to accepting connections. The ``beast`` frontend is
not restricted by the thread pool size when it comes to accepting new
connections, so a scheduler abstraction is introduced in Nautilus release which
for supporting ways for scheduling requests in the future.

Currently the scheduler defaults to a throttler which throttles the active
connections to a configured limit. QoS based on mClock is currently in an
*experimental* phase and not recommended for production yet. Current
implementation of *dmclock_client* op queue divides RGW Ops on admin, auth
(swift auth, sts) metadata & data requests.


``rgw max concurrent requests``

:Description: Maximum number of concurrent HTTP requests that the beast frontend
              will process. Tuning this can help to limit memory usage under
              heavy load.
:Type: Integer
:Default: 1024


``rgw scheduler type``

:Description: The type of RGW Scheduler to use. Valid values are throttler,
              dmclock. Currently defaults to throttler which throttles beast
              frontend requests. dmclock is *experimental* and will need the
              experimental flag set


The options below are to tune the experimental dmclock scheduler. For some
further reading on dmclock, see :ref:`dmclock-qos`. `op_class` for the flags below is
one of admin, auth, metadata or data.

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
