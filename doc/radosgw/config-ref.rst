======================================
 Ceph Object Gateway Config Reference
======================================

The following settings may added to the Ceph configuration file (i.e., usually
``ceph.conf``) under the ``[client.radosgw.gateway]`` section. The settings may
contain default values. If you do not specify each setting in the Ceph
configuration file, the default value will be set automatically.


``rgw data``

:Description: Sets the location of the data files for Ceph Object Gateway.
:Type: String
:Default: ``/var/lib/ceph/radosgw/$cluster-$id``


``rgw enable apis``

:Description: Enables the specified APIs.
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

:Description: The DNS name of the served domain.
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



``rgw swift url``

:Description: The URL for the Ceph Object Gateway Swift API.
:Type: String
:Default: None
	

``rgw swift url prefix``

:Description: The URL prefix for the Swift API. 
:Default: ``swift``
:Example: http://fqdn.com/swift
	

``rgw swift auth url``

:Description: Default URL for verifying v1 auth tokens (if not using internal 
              Swift auth).

:Type: String
:Default: None


``rgw swifth auth entry``

:Description: The entry point for a Swift auth URL.
:Type: String
:Default: ``auth``


``rgw keystone url``

:Description: The URL for the Keystone server.
:Type: String
:Default: None


``rgw keystone admin token``

:Description: The Keystone admin token (shared secret).
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


``rgw admin entry``

:Description: The entry point for an admin request URL.
:Type: String
:Default: ``admin``
	

``rgw enforce swift acls``

:Description: Enforces the Swift Access Control List (ACL) settings.
:Type: Boolean
:Default: ``true``
	
	
``rgw swift token expiration``

:Description: The time in seconds for expiring a Swift token.
:Type: Integer
:Default: ``24 * 3600``


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
              process dies. Disbled if set to ``0``.

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


.. deprecated:: v.67

``rgw cluster root pool``

:Description: The Ceph Storage Cluster pool to store ``radosgw`` metadata for 
              this instance. Not used in Ceph version v.67 and later. Use
              ``rgw zone root pool`` instead.

:Type: String
:Required: No
:Default: ``.rgw.root``
:Replaced By: ``rgw zone root pool``


.. versionadded:: v.67

``rgw zone``

:Description: The name of the zone for the gateway instance.
:Type: String
:Default: None


.. versionadded:: v.67

``rgw zone root pool``

:Description: The pool for storing zone-specific information.
:Type: String
:Default: ``.rgw.root``


.. versionadded:: v.67

``rgw region``

:Description: The name of the region for the gateway instance.
:Type: String
:Default: None

.. versionadded:: v.67

``rgw region root pool``

:Description: The pool for storing all region-specific information.
:Type: String
:Default: ``.rgw.root``


.. versionadded:: v.67

``rgw default region info oid``

:Description: The OID for storing the default region.
:Type: String
:Default: ``default.region``


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


``rgw object stripe size``

:Description: The size of an object stripe for Ceph Object Gateway objects.
              See `Architecture`_ for details on striping.

:Type: Integer
:Default: ``4 << 20``


``rgw extended http attrs``

:Description: Add new set of attributes that could be set on an object. These 
              extra attributes can be set through HTTP header fields when 
              putting the objects. If set, these attributes will return as HTTP 
              fields when doing GET/HEAD on the object.

:Type: String
:Default: None
:Example: "content_foo, content_bar"


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


``rgw md log max shards``

:Description: The maximum number of shards for the metadata log.
:Type: Integer
:Default: ``64``


``rgw num zone opstate shards``

:Description: The maximum number of shards for keeping inter-region copy 
              progress information.

:Type: Integer
:Default: ``128``


``rgw opstate ratelimit sec``

:Description: The minimum time between opstate updates on a single upload. 
              ``0`` disables the ratelimit.

:Type: Integer
:Default: ``30``


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


``rgw data log window``

:Description: The data log entries window in seconds.
:Type: Integer
:Default: ``30``


``rgw data log changes size``

:Description: The number of in-memory entries to hold for the data changes log.
:Type: Integer
:Default: ``1000``


``rgw data log num shards``

:Description: The number of shards (objects) on which to keep the 
              data changes log.

:Type: Integer
:Default: ``128``


``rgw data log obj prefix``

:Description: The object name prefix for the data log.
:Type: String
:Default: ``data_log``


``rgw replica log obj prefix``

:Description: The object name prefix for the replica log.
:Type: String
:Default: ``replica log``


.. _Architecture: ../../architecture#data-striping