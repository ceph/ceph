.. _mon-dns-lookup:

===============================
Looking up Monitors through DNS
===============================

Clients and daemons can look up the Monitors through DNS.

The addition of the ability to look up Monitors through DNS means that daemons
and clients do not require a :confval:`mon_host` configuration directive in their
``ceph.conf`` configuration file.

Clients and daemons look up the Monitors in ``DNS SRV TCP`` records only when
their configuration gives no Monitor addresses, for example when
:confval:`mon_host` is not set. Host names in ``mon_host`` are resolved with
ordinary address lookups, not SRV records. If there are no SRV records either,
the client or daemon cannot find a Monitor and fails with an error. Clients and
daemons read the SRV records when they start, to find their first Monitor.
After startup, clients update their monitor maps from the map that the
Monitors maintain, so a change to the SRV records reaches only processes that
start after the change.

By default, clients and daemons look for the TCP service called *ceph-mon*,
which is configured by the ``mon_dns_srv_name`` configuration directive.


.. confval:: mon_dns_srv_name

.. note:: Instead of relying on the DNS search domain, you can name the domain
   in ``mon_dns_srv_name``: append an underscore and the domain to the service
   name. The syntax for this is
   ``<service-name>_<upper-level-domain>``. For example, passing
   ``ceph-mon_example.com`` will direct Ceph to look for the ``SRV`` record at
   ``_ceph-mon._tcp.example.com``.

Example
-------
When the DNS search domain is set to *example.com*, a DNS zone file might contain the following elements.

First, create address records for the Monitors. Clients and daemons look up
IPv4 (A) records for the SRV targets, or IPv6 (AAAA) records when
``ms_bind_ipv6`` is ``true`` (the default is ``false``).

::

    mon1.example.com. AAAA 2001:db8::100
    mon2.example.com. AAAA 2001:db8::200
    mon3.example.com. AAAA 2001:db8::300

::

    mon1.example.com. A 192.168.0.1
    mon2.example.com. A 192.168.0.2
    mon3.example.com. A 192.168.0.3


With those records now existing we can create the SRV TCP records with the name *ceph-mon* pointing to the three Monitors.

::

    _ceph-mon._tcp.example.com. 60 IN SRV 10 20 3300 mon1.example.com.
    _ceph-mon._tcp.example.com. 60 IN SRV 10 30 3300 mon2.example.com.
    _ceph-mon._tcp.example.com. 60 IN SRV 20 50 3300 mon3.example.com.

These records tell clients to make first contact on port 3300, with
priorities 10, 10, 20 and weights 20, 30, 50 respectively. Ceph reads the
protocol from the port: 3300 means msgr2 (v2), 6789 means msgr1 (v1), and any
other port is tried with both. Ceph uses one SRV record per target host name.
Daemons and user-space clients that make first contact with v1 reconnect with
v2 once they have the monitor map, if the Monitors have v2 addresses. Linux
kernel clients connect with one protocol only: a kernel CephFS mount or
``rbd device map`` with ``ms_mode=legacy`` needs v1 records (port 6789), and
one with any other ``ms_mode`` value needs v2 records (port 3300).
By default, ``ms_mode=prefer-crc`` is assumed.

Monitor clients choose a Monitor by referencing the SRV records. They first try
the Monitors with the lowest priority value: they open connections to up to
``mon_client_hunt_parallel`` of them at once (3 by default), in an order
shuffled by the SRV weight fields, and keep the first that completes
authentication. The weights decide the split only when
``mon_client_hunt_parallel`` is lower than the number of Monitors that share
the lowest priority.

For the above example, a client tries mon1 and mon2 together. However, if
neither of them is reachable, then mon3 will be reconsidered as a fallback.

See also :ref:`msgr2`.

