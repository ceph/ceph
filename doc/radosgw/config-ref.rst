=======================================
 RADOS Gateway Configuration Reference
=======================================

The following settings may added to the ``ceph.conf`` file under the
``[client.radosgw.gateway]`` section. The settings may contain default values.
If you do not specify each setting in ``ceph.conf``, the default value will be
set automatically.


``rgw data``

:Description: Sets the location of the data files for RADOS Gateway.
:Type: String
:Default: ``/var/lib/ceph/radosgw/$cluster-$id``


``rgw cache enabled``

:Description: Whether the RADOS Gateway cache is enabled.
:Type: Boolean
:Default: ``true``


``rgw cache lru size``

:Description: The number of entries in the RADOS Gateway cache.
:Type: Integer
:Default: ``10000``
	

``rgw socket path``

:Description: The socket path for the domain socket. ``FastCgiExternalServer`` 
              uses this socket. If you do not specify a socket path, RADOS 
              Gateway will not run as an external server. The path you specify 
              here must be the same as the path specified in the ``rgw.conf`` 
              file.

:Type: String
:Default: N/A


``rgw dns name``

:Description: The DNS name of the served domain.
:Type: String 
:Default: None
	

``rgw swift url``

:Description: The URL for the RADOS Gateway Swift API.
:Type: String
:Default: None
	

``rgw swift url prefix``

:Description: The URL prefix for the Swift API. 
:Default: ``swift``
:Example: http://fqdn.com/swift
	

``rgw enforce swift acls``

:Description: Enforces the Swift Access Control List (ACL) settings.
:Type: Boolean
:Default: ``true``
	

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
	
:Description: The time ``timeout`` in seconds before a RADOS Gateway process 
              dies. Disbled if set to ``0``.

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


``rgw log nonexistent bucket``

:Description: Enables RADOS Gateway to log a request for a non-existent bucket.
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

:Description: Enable logging for RGW operations.
:Type: Boolean
:Default: ``true``


``rgw enable usage log``

:Description: Enable the usage log.
:Type: Boolean
:Default: ``true``


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

:Description: The number of seconds before RGW gives up on initialization.
:Type: Integer
:Default: ``30``


``rgw mime types file``

:Description: The path and location of the MIME types. Used for Swift 
              auto-detection of object types.

:Type: String
:Default: ``/etc/mime.types``


``rgw resolve cname``

:Description: Whether ``rgw`` should use DNS CNAME record of the request hostname 
              field (if hostname is not equal to ``rgw dns name``).

:Type: Boolean
:Default: ``false``


``rgw enable ops log``

:Description: Whether rgw will log each successful operation
:Type: Boolean
:Default: ``true``

``rgw ops log rados``

:Description: Whether ``rgw`` operations logging should be written into the 
              RADOS backend.

:Type: Boolean
:Default: ``true``

``rgw ops log socket path``

:Description: The path of a UNIX domain socket to which operations logging 
              data will be written.

:Type: String
:Default: N/A

``rgw ops log data backlog``

:Description: Total backlog data size for UNIX domain socket 
              operations logging.

:Type: Integer
:Default: ``5ul << 20``

``rgw extended http attrs``

:Description: Add new set of attributes that could be set on an object. These 
              extra attributes can be set through HTTP header fields when 
              putting the objects. If set, these attributes will return as HTTP 
              fields when doing GET/HEAD on the object.

:Type: String
:Default: N/A
:Example: "content_foo, content_bar"


``rgw cluster root pool``

:Description: RADOS pool to store ``radosgw`` metadata for this instance.
:Type: String
:Required: No
:Default: ``.rgw.root``


``rgw gc max objs``

:Description: Number of objects to collect garbage collection data.
:Type: 32-bit Integer
:Default: 32


``rgw gc obj min wait``

:Description: Minimum time to wait before object's removal and its processing 
              by the garbage collector.

:Type: 32-bit Integer
:Default: 2 hours.  ``2*60*60``


``rgw gc processor max time``

:Description: Max time for a single garbage collection process run.
:Type: 32-bit Integer
:Default: 1 hour.  ``60*60``


``rgw gc processor max period``

:Description: Max time between the beginning of two consecutive garbage 
              collection processes run.

:Type: 32-bit Integer
:Default: 1 hour.  ``60*60``
