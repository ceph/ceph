:orphan:

===============================
 radosgw -- RADOS REST gateway
===============================

.. program:: radosgw

Synopsis
========

| **radosgw**


Description
===========

:program:`radosgw` is an HTTP REST gateway for the RADOS object store, a part
of the Ceph distributed storage system. It serves S3-compatible and
Swift-compatible APIs through its built-in HTTP frontend, Beast, and does not
need a separate web server.


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine Monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified Monitor (instead of looking through ``ceph.conf``).

.. option:: -i ID, --id ID

   Set the ID portion of the name for radosgw

.. option:: -n TYPE.ID, --name TYPE.ID

   Set the RADOS user name for the gateway (e.g. client.radosgw.gateway)

.. option:: --cluster NAME

   Set the cluster name (default: ceph)

.. option:: -d

   Run in foreground, log to stderr

.. option:: -f

   Run in foreground, log to usual location

.. option:: -k keyring, --keyring=keyring

   Read the gateway's key from ``keyring`` instead of the default,
   ``$rgw_data/keyring``, which is
   ``/var/lib/ceph/radosgw/$cluster-$id/keyring`` unless ``rgw_data`` is set.

.. option:: --setuser USER

   Run as ``USER``, a user name or a UID. :program:`radosgw` switches to this
   user only after Beast has bound its ports, so it can listen on a port
   below 1024. If a user name is given, its primary group is used as well,
   unless :option:`--setgroup` is also given.

.. option:: --setgroup GROUP

   Run as ``GROUP``, a group name or a GID.

.. option:: --rgw-realm=realm

   The realm where radosgw runs

.. option:: --rgw-zonegroup=zonegroup

   The zonegroup where radosgw runs

.. option:: --rgw-zone=zone

   The zone where radosgw runs

.. option:: --rgw-region=region

   Obsolete name for :option:`--rgw-zonegroup`. It is used only when
   ``--rgw-zonegroup`` is not set.

Any other configuration option can also be given on the command line, as
``--option-name=value``. For example, ``--debug-rgw=20`` raises the gateway's
log level.


Configuration
=============

The ``rgw_frontends`` option configures the frontend. Its default value,
``beast port=7480``, makes :program:`radosgw` listen on port 7480. To listen on
port 8080 instead, put the following in the gateway's section of
``ceph.conf``::

        [client.radosgw.gateway]
            rgw_frontends = beast port=8080

For the other Beast options, including SSL, see
https://docs.ceph.com/en/latest/radosgw/frontends/. For the other gateway
options, see https://docs.ceph.com/en/latest/radosgw/config-ref/.

In a cluster that cephadm manages, run ``ceph orch apply rgw`` to deploy
gateways instead of starting :program:`radosgw` by hand. See
https://docs.ceph.com/en/latest/cephadm/services/rgw/. To deploy a gateway by
hand, including its key and its systemd service, see the RGW section of
https://docs.ceph.com/en/latest/install/manual-deployment/.

Usage Logging
=============

:program:`radosgw` maintains an asynchronous usage log. It accumulates
statistics about user operations and flushes it periodically. The
logs can be accessed and managed through :program:`radosgw-admin`.

The information that is being logged contains total data transfer,
total operations, and total successful operations. The data is being
accounted in an hourly resolution under the bucket owner, unless the
operation was done on the service (e.g., when listing a bucket) in
which case it is accounted under the operating user.

Following is an example configuration::

        [client.radosgw.gateway]
            rgw_enable_usage_log = true
            rgw_usage_log_tick_interval = 30
            rgw_usage_log_flush_threshold = 1024
            rgw_usage_max_shards = 32
            rgw_usage_max_user_shards = 1


The total number of shards determines how many total objects hold the
usage log information. The per-user number of shards specifies how many
objects hold usage information for a single user. The tick interval
configures the number of seconds between log flushes, and the flush
threshold specifies how many entries can be kept before resorting to
synchronous flush.

.. warning:: All RGW instances and ``radosgw-admin`` commands must use the
   same value for ``rgw_usage_max_shards``. If values differ between daemons
   or between the cluster and the admin tool, usage log reads and trims
   will target different objects than where data was written, causing
   seemingly empty results or failed cleanup. Use
   ``ceph config set global rgw_usage_max_shards <N>`` to enforce a
   consistent value across the cluster. Alternatively, ``radosgw-admin``
   supports the ``--rgw-usage-max-shards`` command-line parameter.


Availability
============

:program:`radosgw` is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at https://docs.ceph.com for
more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`radosgw-admin <radosgw-admin>`\(8)
