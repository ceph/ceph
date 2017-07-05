===============================
Looking op Monitors through DNS
===============================

Since version 11.0.0 RADOS supports looking up Monitors through DNS.

This way daemons and clients do not require a *mon host* configuration directive in their ceph.conf configuration file.

Using DNS SRV TCP records clients are able to look up the monitors.

This allows for less configuration on clients and monitors. Using a DNS update clients and daemons can be made aware of changes in the monitor topology.

By default clients and daemons will look for the TCP service called *ceph-mon* which is configured by the *mon_dns_srv_name* configuration directive.


``mon dns srv name``

:Description: the service name used querying the DNS for the monitor hosts/addresses
:Type: String
:Default: ``ceph-mon``

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

    _ceph-mon._tcp.example.com. 60 IN SRV 10 60 6789 mon1.example.com.
    _ceph-mon._tcp.example.com. 60 IN SRV 10 60 6789 mon2.example.com.
    _ceph-mon._tcp.example.com. 60 IN SRV 10 60 6789 mon3.example.com.

In this case the Monitors are running on port *6789*, and their priority and weight are all *10* and *60* respectively.

The current implementation in clients and daemons will *only* respect the priority set in SRV records, and they will only connect to the monitors with lowest-numbered priority. The targets with the same priority will be selected at random.
