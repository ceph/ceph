=======================================
 RADOS Gateway Configuration Reference
=======================================

The following settings may added to the ``ceph.conf`` file. The settings may 
contain default values. If you do not specify each setting in ``ceph.conf``,
the default value will be set automatically.

``rgw data``

:Description: Sets the location of the data file for RADOS Gateway.
:Default: ``/var/lib/ceph/radosgw/$cluster-$id``

``rgw cache enabled``

:Description: Whether the RADOS Gateway cache is enabled.
:Default: ``true``

``rgw cache lru size``

:Description: The number of entries in the RADOS Gateway cache.
:Default: ``10000``
	
``rgw socket path``

:Description: The socket path for the domain socket. ``FastCgiExternalServer`` uses this socket. If you do not specify a socket path, RADOS Gateway will not run as an external server. The path you specify here must be the same as the path specified in the ``rgw.conf`` file.
:Default: N/A
:Required: True
:Example: ``/var/run/ceph/rgw.sock``

``rgw dns name``

:Description: The name of the DNS host. 
:Default: Same as the host's DNS.
	
``rgw swift url``

:Description: The URL for the RADOS Gateway Swift API.
:Default: Same as the ``host`` setting.
	
``rgw swift url prefix``

:Description: The URL prefix for the Swift API. 
:Default: ``swift``
:Example: http://swift.fqdn.com
	
``rgw enforce swift acls``

:Description: Enforces the Swift Access Control List (ACL) settings.
:Default: ``true``
	
``rgw print continue``

:Description: Enable ``100-continue`` if it is operational.
:Default: ``true``

``rgw remote addr param``

:Description: The remote address parameter. For example, a variable for the ``X-Forwarded-For`` address if a reverse proxy is operational.
:Default: ``REMOTE_ADDR``
	
``rgw op thread timeout``
	
:Description: The timeout in milliseconds for open threads.
:Default: 600
	
``rgw op thread suicide timeout``
	
:Description: <placeholder>	
:Default: <placeholder>

``rgw thread pool size``

:Description: The size of the thread pool. 
:Default: 100 threads.
	
``rgw maintenance tick interval``

:Description: <placeholder>
:Default: 10.0

``rgw pools preallocate max``

:Description: The maximum number of pool to pre-allocate to RADOS Gateway.
:Default: 100

``rgw pools preallocate threshold``

:Description: The pool pre-allocation threshold. <placeholder>
:Default: 70

``rgw log nonexistent bucket``

:Description: Should RADOS GW log a request for a non-existent bucket?
:Default: ``false``

``rgw log object name``

:Description: The logging format for an object name.  // man date to see codes (a subset are supported)
:Default: "%Y-%m-%d-%H-%i-%n"


``rgw log object name utc``

:Description: <placeholder>
:Default: ``false``


``rgw usage max shards``

:Description: The maximum number of shards.
:Default: 32

``rgw usage max user shards``

:Description: The maximum number of shards per user.
:Default: 1

``rgw enable ops log``

:Description: Enable logging for every RGW operation?
:Default: ``true``

``rgw enable usage log``

:Description: Log bandwidth usage?
:Default: ``true``

``rgw usage log flush threshold``

:Description: The threshold to flush pending log data.
:Default: 1024


``rgw usage log tick interval``

:Description: Flush pending log data every ``n`` seconds.
:Default: 30

``rgw intent log object name``

:Description: The logging format for <placeholder>. // man date to see codes (a subset are supported)
:Default: "%Y-%m-%d-%i-%n"

``rgw intent log object name utc``

:Description: Whether the intent log object name should use Coordinated Universal Time (UTC).
:Default: ``false``

``rgw init timeout``

:Description: The timeout threshold in seconds.
:Default: 30

``rgw mime types file``

:Description: The path and location of the MIME types.
:Default: ``/etc/mime.types``
