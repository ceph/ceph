======================================
 Ceph Object Gateway Config Reference
======================================

The following settings may added to the Ceph configuration file (i.e., usually
``ceph.conf``) under the ``[client.radosgw.{instance-name}]`` section. The
settings may contain default values. If you do not specify each setting in the
Ceph configuration file, the default value will be set automatically.


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


``rgw admin entry``

:Description: The entry point for an admin request URL.
:Type: String
:Default: ``admin``


``rgw content length compat``

:Description: Enable compatability handling of FCGI requests with both CONTENT_LENGTH AND HTTP_CONTENT_LENGTH set.
:Type: Boolean
:Default: ``false``

Regions
=======

In Ceph v0.67 and beyond, Ceph Object Gateway supports federated deployments and
a global namespace via the notion of regions. A region defines the geographic
location of one or more Ceph Object Gateway instances within one or more zones. 


Configuring regions differs from typical configuration procedures, because not
all of the settings end up in a Ceph configuration file. In Ceph v0.67 and
beyond, you can list regions, get a region configuration and set a region
configuration.


List Regions
------------

A Ceph cluster contains a list of regions. To list the regions, execute:: 

	sudo radosgw-admin regions list

The ``radosgw-admin`` returns a JSON formatted list of regions. 

.. code-block:: javascript

	{ "default_info": { "default_region": "default"},
	  "regions": [
	        "default"]}
	        

Get a Region Map
----------------

To list the details of each region, execute:: 

	sudo radosgw-admin region-map get
	
	
.. note:: If you receive a ``failed to read region map`` error, run
   ``sudo radosgw-admin region-map update`` first.


Get a Region
------------

To view the configuration of a region, execute:: 

	radosgw-admin region get [--rgw-region=<region>]

The ``default`` region looks like this:

.. code-block:: javascript

   {"name": "default",
    "api_name": "",
    "is_master": "true",
    "endpoints": [],
    "master_zone": "",
    "zones": [
      {"name": "default",
       "endpoints": [],
       "log_meta": "false",
       "log_data": "false"}
     ],
    "placement_targets": [
      {"name": "default-placement",
       "tags": [] }],
    "default_placement": "default-placement"}

Set a Region
------------

Defining a region consists of creating a JSON object, specifying at least the
required settings:

#. ``name``: The name of the region. Required.

#. ``api_name``: The API name for the region. Optional.

#. ``is_master``: Determines if the region is the master region.  Required.
   **note:** You can only have one master region.

#. ``endpoints``: A list of all the endpoints in the region. For example, 
   you may use multiple domain names to refer to the same region. Remember to 
   escape the forward slashes (``\/``). You may also specify a 
   port (``fgdn:port``) for each endpoint. Optional.

#. ``master_zone``: The master zone for the region. Optional. Uses the default
   zone if not specified. **note:** You can only have one master zone per 
   region.

#. ``zones``: A list of all zones within the region. Each zone has a 
   name (required), a list of endpoints (optional), and whether or not the 
   gateway will log metadata and data operations (false by default).

#. ``placement_targets``: A list of placement targets (optional). Each 
   placement target contains a name (required) for the placement target 
   and a list of tags (optional) so that only users with the tag can use
   the placement target (i.e., the user's ``placement_tags`` field in the 
   user info). 

#. ``default_placement``: The default placement target for the object 
   index and object data. Set to ``default-placement`` by default. You 
   may also set a per-user default placement in the user info for each 
   user.

To set a region, create a JSON object consisting of the required fields, save
the object to a file (e.g., ``region.json``); then, execute the following
command::

	sudo radosgw-admin region set --infile region.json

Where ``region.json`` is the JSON file you created.


.. important:: The ``default`` region ``is_master`` setting is ``true`` by
   default. If you create a new region and want to make it the master region,
   you must either set the ``default`` region ``is_master`` setting to 
   ``false``, or delete the ``default`` region.


Finally, update the map. :: 

	sudo radosgw-admin region-map update


Set a Region Map
----------------

Setting a region map consists of creating a JSON object consisting of one or more
regions, and setting the ``master_region`` for the cluster. Each region in the 
region map consists of a key/value pair, where the ``key`` setting is equivalent to
the ``name`` setting for an individual region configuration, and the ``val`` is 
a JSON object consisting of an individual region configuration.

You may only have one region with ``is_master`` equal to ``true``, and it must be
specified as the ``master_region`` at the end of the region map. The following
JSON object is an example of a default region map.


.. code-block:: javascript

     { "regions": [
          { "key": "default",
            "val": { "name": "default",
            "api_name": "",
            "is_master": "true",
            "endpoints": [],
            "master_zone": "",
            "zones": [
              { "name": "default",
                "endpoints": [],
                "log_meta": "false",
                 "log_data": "false"}],
                 "placement_targets": [
                   { "name": "default-placement",
                     "tags": []}],
                     "default_placement": "default-placement"
                   }
               }
            ],
        "master_region": "default"
     }

To set a region map, execute the following:: 

	sudo radosgw-admin region-map set --infile regionmap.json

Where ``regionmap.json`` is the JSON file you created. Ensure that you have
zones created for the ones specified in the region map. Finally, update the map.
::

	sudo radosgw-admin regionmap update


Zones
=====

In Ceph v0.67 and beyond, Ceph Object Gateway supports the notion of zones. A
zone defines a logical group consisting of one or more Ceph Object Gateway
instances.

Configuring zones differs from typical configuration procedures, because not
all of the settings end up in a Ceph configuration file. In Ceph v0.67 and
beyond, you can list zones, get a zone configuration and set a zone
configuration.


List Zones
----------

To list the zones in a cluster, execute::

	sudo radosgw-admin zone list


Get a Zone
----------

To get the configuration of a zone, execute:: 

	sudo radosgw-admin zone get [--rgw-zone=<zone>]

The ``default`` zone looks like this:

.. code-block:: javascript

   { "domain_root": ".rgw",
     "control_pool": ".rgw.control",
     "gc_pool": ".rgw.gc",
     "log_pool": ".log",
     "intent_log_pool": ".intent-log",
     "usage_log_pool": ".usage",
     "user_keys_pool": ".users",
     "user_email_pool": ".users.email",
     "user_swift_pool": ".users.swift",
     "user_uid_pool": ".users.uid",
     "system_key": { "access_key": "", "secret_key": ""},
     "placement_pools": [
         {  "key": "default-placement",
            "val": { "index_pool": ".rgw.buckets.index",
                     "data_pool": ".rgw.buckets"}
         }
       ]
     }


Set a Zone
----------

Configuring a zone involves specifying a series of Ceph Object Gateway pools.
For consistency, we recommend using a pool prefix that is
the same as the zone name. See `Pools`_ for details of configuring pools.

To set a zone, create a JSON object consisting of the pools, save
the object to a file (e.g., ``zone.json``); then, execute the following
command, replacing ``{zone-name}`` with the name of the zone::

	sudo radosgw-admin zone set --rgw-zone={zone-name} --infile zone.json

Where ``zone.json`` is the JSON file you created.


Region/Zone Settings
====================

You may include the following settings in your Ceph configuration
file under each ``[client.radosgw.{instance-name}]`` instance.


.. versionadded:: v.67

``rgw zone``

:Description: The name of the zone for the gateway instance.
:Type: String
:Default: None


.. versionadded:: v.67

``rgw region``

:Description: The name of the region for the gateway instance.
:Type: String
:Default: None


.. versionadded:: v.67

``rgw default region info oid``

:Description: The OID for storing the default region. We do not recommend
              changing this setting.
              
:Type: String
:Default: ``default.region``



Pools
=====

Ceph zones map to a series of Ceph Storage Cluster pools. 

.. topic:: Manually Created Pools vs. Generated Pools

   If you provide write capabilities to the user key for your Ceph Object 
   Gateway, the gateway has the ability to create pools automatically. This 
   is convenient, but the Ceph Object Storage Cluster uses the default 
   values for the number of placement groups (which may not be ideal) or the 
   values you specified in your Ceph configuration file. If you allow the 
   Ceph Object Gateway to create pools automatically, ensure that you have 
   reasonable defaults for the number of placement groups. See 
   `Pool Configuration`_ for details. See `Cluster Pools`_ for details on 
   creating pools.
   
The default pools for the Ceph Object Gateway's default zone include:

- ``.rgw``
- ``.rgw.control``
- ``.rgw.gc``
- ``.log``
- ``.intent-log``
- ``.usage``
- ``.users``
- ``.users.email``
- ``.users.swift``
- ``.users.uid``

You have significant discretion in determining how you want a zone to access
pools. You can create pools on a per zone basis, or use the same pools for
multiple zones. As a best practice, we recommend having a separate set of pools
for your master zone and your secondary zones in each region. When creating
pools for a specific zone, consider prepending the region name and zone name to
the default pool names. For example:

- ``.region1-zone1.domain.rgw``
- ``.region1-zone1.rgw.control``
- ``.region1-zone1.rgw.gc``
- ``.region1-zone1.log``
- ``.region1-zone1.intent-log``
- ``.region1-zone1.usage``
- ``.region1-zone1.users``
- ``.region1-zone1.users.email``
- ``.region1-zone1.users.swift``
- ``.region1-zone1.users.uid``


Ceph Object Gateways store data for the bucket index (``index_pool``) and bucket
data (``data_pool``) in placement pools. These may overlap--i.e., you may use
the same pool for the the index and the data. The index pool for default
placement is ``.rgw.buckets.index`` and for the data pool for default placement
is ``.rgw.buckets``. See `Zones`_ for details on specifying pools in a zone
configuration.


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

``rgw region root pool``

:Description: The pool for storing all region-specific information.
:Type: String
:Default: ``.rgw.root``



.. versionadded:: v.67

``rgw zone root pool``

:Description: The pool for storing zone-specific information.
:Type: String
:Default: ``.rgw.root``


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

:Description: The URL prefix for the Swift API. 
:Default: ``swift``
:Example: http://fqdn.com/swift
	

``rgw swift auth url``

:Description: Default URL for verifying v1 auth tokens (if not using internal 
              Swift auth).

:Type: String
:Default: None


``rgw swift auth entry``

:Description: The entry point for a Swift auth URL.
:Type: String
:Default: ``auth``



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


``rgw md log max shards``

:Description: The maximum number of shards for the metadata log.
:Type: Integer
:Default: ``64``



Keystone Settings
=================


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



.. _Architecture: ../../architecture#data-striping
.. _Pool Configuration: ../../rados/configuration/pool-pg-config-ref/
.. _Cluster Pools: ../../rados/operations/pools
