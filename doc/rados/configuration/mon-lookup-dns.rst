.. _mon-dns-lookup:

===============================
Looking up Monitors through DNS
===============================

Since Ceph version 11.0.0 (Kraken), RADOS has supported looking up monitors
through DNS.

The addition of the ability to look up monitors through DNS means that daemons
and clients do not require a *mon host* configuration directive in their
``ceph.conf`` configuration file.

With a DNS update, clients and daemons can be made aware of changes
in the monitor topology. To be more precise and technical, clients look up the
monitors by using ``DNS SRV TCP`` records. 

By default, clients and daemons look for the TCP service called *ceph-mon*,
which is configured by the *mon_dns_srv_name* configuration directive.


.. confval:: mon_dns_srv_name

Example
-------
When the DNS search domain is set to *example.com* a DNS zone file might contain the following elements.

First, create records for the Monitors, either IPv4 (A) or IPv6 (AAAA).

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

    _ceph-mon._tcp.example.com. 60 IN SRV 10 20 6789 mon1.example.com.
    _ceph-mon._tcp.example.com. 60 IN SRV 10 30 6789 mon2.example.com.
    _ceph-mon._tcp.example.com. 60 IN SRV 20 50 6789 mon3.example.com.

Now all Monitors are running on port *6789*, with priorities 10, 10, 20 and weights 20, 30, 50 respectively.

Monitor clients choose monitor by referencing the SRV records. If a cluster has multiple Monitor SRV records
with the same priority value, clients and daemons will load balance the connections to Monitors in proportion
to the values of the SRV weight fields.

For the above example, this will result in approximate 40% of the clients and daemons connecting to mon1,
60% of them connecting to mon2. However, if neither of them is reachable, then mon3 will be reconsidered as a fallback.
