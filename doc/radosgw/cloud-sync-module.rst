=========================
Cloud Sync Module
=========================

.. versionadded:: Mimic

This module syncs zone data to a remote cloud service. The sync is unidirectional; data is not synced back from the
remote zone. The goal of this module is to enable syncing data to multiple cloud providers. The currently supported
cloud providers are those that are compatible with AWS (S3).

User credentials for the remote cloud object store service need to be configured. Since many cloud services impose limits
on the number of buckets that each user can create, the mapping of source objects and buckets is configurable.
It is possible to configure different targets to different buckets and bucket prefixes. Note that source ACLs will not
be preserved. It is possible to map permissions of specific source users to specific destination users.

Due to API limitations there is no way to preserve original object modification time and ETag. The cloud sync module 
stores these as metadata attributes on the destination objects.



Cloud Sync Tier Type Configuration
-------------------------------------

Trivial Configuration:
~~~~~~~~~~~~~~~~~~~~~~

::

    {
      "connection": {
        "access_key": <access>,
        "secret": <secret>,
        "endpoint": <endpoint>,
        "host_style": <path | virtual>,
      },
      "acls": [ { "type": <id | email | uri>,
                  "source_id": <source_id>,
                  "dest_id": <dest_id> } ... ],
      "target_path": <target_path>,
    }


Non Trivial Configuration:
~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    {
      "default": {
        "connection": {
            "access_key": <access>,
            "secret": <secret>,
            "endpoint": <endpoint>,
            "host_style" <path | virtual>,
        },
        "acls": [
        {
          "type" : <id | email | uri>,   #  optional, default is id
          "source_id": <id>,
          "dest_id": <id>
        } ... ]
        "target_path": <path> # optional
      },
      "connections": [
          {
            "connection_id": <id>,
            "access_key": <access>,
            "secret": <secret>,
            "endpoint": <endpoint>,
            "host_style" <path | virtual>,  # optional
          } ... ],
      "acl_profiles": [
          {
            "acls_id": <id>, # acl mappings
            "acls": [ {
                "type": <id | email | uri>,
                "source_id": <id>,
                "dest_id": <id>
              } ... ]
          }
      ],
      "profiles": [
          {
           "source_bucket": <source>,
           "connection_id": <connection_id>,
           "acls_id": <mappings_id>,
           "target_path": <dest>,          # optional
          } ... ],
    }


.. Note:: Trivial configuration can coincide with the non-trivial one.


* ``connection`` (container)

Represents a connection to the remote cloud service. Contains ``conection_id`, ``access_key``,
``secret``, ``endpoint``, and ``host_style``.

* ``access_key`` (string)

The remote cloud access key that will be used for a specific connection.

* ``secret`` (string)

The secret key for the remote cloud service.

* ``endpoint`` (string)

URL of remote cloud service endpoint.

* ``host_style`` (path | virtual)

Type of host style to be used when accessing remote cloud endpoint (default: ``path``).

* ``acls`` (array)

Contains a list of ``acl_mappings``.

* ``acl_mapping`` (container)

Each ``acl_mapping`` structure contains ``type``, ``source_id``, and ``dest_id``. These
will define the ACL mutation that will be done on each object. An ACL mutation allows converting source
user id to a destination id.

* ``type`` (id | email | uri)

ACL type: ``id`` defines user id, ``email`` defines user by email, and ``uri`` defines user by ``uri`` (group).

* ``source_id`` (string)

ID of user in the source zone.

* ``dest_id`` (string)

ID of user in the destination.

* ``target_path`` (string)

A string that defines how the target path is created. The target path specifies a prefix to which
the source object name is appended. The target path configurable can include any of the following
variables:
- ``sid``: unique string that represents the sync instance ID
- ``zonegroup``: the zonegroup name
- ``zonegroup_id``: the zonegroup ID
- ``zone``: the zone name
- ``zone_id``: the zone id
- ``bucket``: source bucket name
- ``owner``: source bucket owner ID

For example: ``target_path = rgwx-${zone}-${sid}/${owner}/${bucket}``


* ``acl_profiles`` (array)

An array of of ``acl_profile``.

* ``acl_profile`` (container)
 
Each profile contains ``acls_id`` (string) that represents the profile, and ``acls`` array that
holds a list of ``acl_mappings``.

* ``profiles`` (array)

A list of profiles. Each profile contains the following:
- ``source_bucket``: either a bucket name, or a bucket prefix (if ends with ``*``) that defines the source bucket(s) for this profile
- ``target_path``: as defined above
- ``connection_id``: ID of the connection that will be used for this profile
- ``acls_id``: ID of ACLs profile that will be used for this profile


S3 Specific Configurables:
~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently cloud sync will only work with backends that are compatible with AWS S3. There are
a few configurables that can be used to tweak its behavior when accessing these cloud services:

::

    {
      "multipart_sync_threshold": {object_size},
      "multipart_min_part_size": {part_size}
    }


* ``multipart_sync_threshold`` (integer)

Objects this size or larger will be synced to the cloud using multipart upload.

* ``multipart_min_part_size`` (integer)

Minimum parts size to use when syncing objects using multipart upload.


How to Configure
~~~~~~~~~~~~~~~~

See `Multisite Configuration`_ for how to multisite config instructions. The cloud sync module requires a creation of a new zone. The zone
tier type needs to be defined as ``cloud``:

::

    # radosgw-admin zone create --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --endpoints={http://fqdn}[,{http://fqdn}]
                                --tier-type=cloud


The tier configuration can be then done using the following command

::

    # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config={key}={val}[,{key}={val}]

The ``key`` in the configuration specifies the config variable that needs to be updated, and
the ``val`` specifies its new value. Nested values can be accessed using period. For example:

::

    # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config=connection.access_key={key},connection.secret={secret}


Configuration array entries can be accessed by specifying the specific entry to be referenced enclosed
in square brackets, and adding new array entry can be done by using `[]`. Index value of `-1` references
the last entry in the array. At the moment it is not possible to create a new entry and reference it
again at the same command.
For example, creating a new profile for buckets starting with {prefix}:

::

    # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config=profiles[].source_bucket={prefix}'*'

    # radosgw-admin zone modify --rgw-zonegroup={zone-group-name} \
                                --rgw-zone={zone-name} \
                                --tier-config=profiles[-1].connection_id={conn_id},profiles[-1].acls_id={acls_id}


An entry can be removed by using ``--tier-config-rm={key}``.


.. _Multisite Configuration: ./multisite
