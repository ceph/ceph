.. _msgr2:

Messenger v2
============

What is it
----------

The messenger v2 protocol, or msgr2, is the second major revision on
Ceph's on-wire protocol.  It brings with it several key features:

* A *secure* mode that encrypts all data passing over the network
* Improved encapsulation of authentication payloads, enabling future
  integration of new authentication modes like Kerberos
* Improved earlier feature advertisement and negotiation, enabling
  future protocol revisions

Ceph daemons can now bind to multiple ports, allowing both legacy Ceph
clients and new v2-capable clients to connect to the same cluster.

By default, monitors now bind to the new IANA-assigned port ``3300``
(ce4h or 0xce4) for the new v2 protocol, while also binding to the
old default port ``6789`` for the legacy v1 protocol.

Address formats
---------------

Prior to nautilus, all network addresses were rendered like
``1.2.3.4:567/89012`` where there was an IP address, a port, and a
nonce to uniquely identify a client or daemon on the network.
Starting with nautilus, we now have three different address types:

* **v2**: ``v2:1.2.3.4:578/89012`` identifies a daemon binding to a
  port speaking the new v2 protocol
* **v1**: ``v1:1.2.3.4:578/89012`` identifies a daemon binding to a
  port speaking the legacy v1 protocol.  Any address that was
  previously shown with any prefix is now shown as a ``v1:`` address.
* **TYPE_ANY** addresses identify a client that can speak either
  version of the protocol.  Prior to nautilus, clients would appear as
  ``1.2.3.4:0/123456``, where the port of 0 indicates they are clients
  and do not accept incoming connections.  Starting with Nautilus,
  these clients are now internally represented by a **TYPE_ANY**
  address, and still shown with no prefix, because they may
  connect to daemons using the v2 or v1 protocol, depending on what
  protocol(s) the daemons are using.

Because daemons now bind to multiple ports, they are now described by
a vector of addresses instead of a single address.  For example,
dumping the monitor map on a Nautilus cluster now includes lines
like::

  epoch 1
  fsid 50fcf227-be32-4bcb-8b41-34ca8370bd16
  last_changed 2019-02-25 11:10:46.700821
  created 2019-02-25 11:10:46.700821
  min_mon_release 14 (nautilus)
  0: [v2:10.0.0.10:3300/0,v1:10.0.0.10:6789/0] mon.foo
  1: [v2:10.0.0.11:3300/0,v1:10.0.0.11:6789/0] mon.bar
  2: [v2:10.0.0.12:3300/0,v1:10.0.0.12:6789/0] mon.baz

The bracketed list or vector of addresses means that the same daemon can be
reached on multiple ports (and protocols).  Any client or other daemon
connecting to that daemon will use the v2 protocol (listed first) if
possible; otherwise it will back to the legacy v1 protocol.  Legacy
clients will only see the v1 addresses and will continue to connect as
they did before, with the v1 protocol.

Starting in Nautilus, the ``mon_host`` configuration option and ``-m
<mon-host>`` command line options support the same bracketed address
vector syntax.


Bind configuration options
^^^^^^^^^^^^^^^^^^^^^^^^^^

Two new configuration options control whether the v1 and/or v2
protocol is used:

  * ``ms_bind_msgr1`` [default: true] controls whether a daemon binds
    to a port speaking the v1 protocol
  * ``ms_bind_msgr2`` [default: true] controls whether a daemon binds
    to a port speaking the v2 protocol

Similarly, two options control whether IPv4 and IPv6 addresses are used:

  * ``ms_bind_ipv4`` [default: true] controls whether a daemon binds
    to an IPv4 address
  * ``ms_bind_ipv6`` [default: false] controls whether a daemon binds
    to an IPv6 address

.. note: The ability to bind to multiple ports has paved the way for
   dual-stack IPv4 and IPv6 support.  That said, dual-stack support is
   not yet tested as of Nautilus v14.2.0 and likely needs some
   additional code changes to work correctly.

Connection modes
----------------

The v2 protocol supports two connection modes:

* *crc* mode provides:

  - a strong initial authentication when the connection is established
    (with cephx, mutual authentication of both parties with protection
    from a man-in-the-middle or eavesdropper), and
  - a crc32c integrity check to protect against bit flips due to flaky
    hardware or cosmic rays

  *crc* mode does *not* provide:

  - secrecy (an eavesdropper on the network can see all
    post-authentication traffic as it goes by) or
  - protection from a malicious man-in-the-middle (who can deliberate
    modify traffic as it goes by, as long as they are careful to
    adjust the crc32c values to match)

* *secure* mode provides:

  - a strong initial authentication when the connection is established
    (with cephx, mutual authentication of both parties with protection
    from a man-in-the-middle or eavesdropper), and
  - full encryption of all post-authentication traffic, including a
    cryptographic integrity check.

  In Nautilus, secure mode uses the `AES-GCM
  <https://en.wikipedia.org/wiki/Galois/Counter_Mode>`_ stream cipher,
  which is generally very fast on modern processors (e.g., faster than
  a SHA-256 cryptographic hash).

Connection mode configuration options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For most connections, there are options that control which modes are used:

* ``ms_cluster_mode`` is the connection mode (or permitted modes) used
  for intra-cluster communication between Ceph daemons.  If multiple
  modes are listed, the modes listed first are preferred.
* ``ms_service_mode`` is a list of permitted modes for clients to use
  when connecting to the cluster.
* ``ms_client_mode`` is a list of connection modes, in order of
  preference, for clients to use (or allow) when talking to a Ceph
  cluster.

There are a parallel set of options that apply specifically to
monitors, allowing administrators to set different (usually more
secure) requirements on communication with the monitors.

* ``ms_mon_cluster_mode`` is the connection mode (or permitted modes)
  to use between monitors.
* ``ms_mon_service_mode`` is a list of permitted modes for clients or
  other Ceph daemons to use when connecting to monitors.
* ``ms_mon_client_mode`` is a list of connection modes, in order of
  preference, for clients or non-monitor daemons to use when
  connecting to monitors.


Transitioning from v1-only to v2-plus-v1
----------------------------------------

By default, ``ms_bind_msgr2`` is true starting with Nautilus 14.2.z.
However, until the monitors start using v2, only limited services will
start advertising v2 addresses.

For most users, the monitors are binding to the default legacy port ``6789`` for the v1 protocol.  When this is the case, enabling v2 is as simple as::

  ceph mon enable-msgr2

If the monitors are bound to non-standard ports, you will need to
specify an additional port for v2 explicitly.  For example, if your
monitor ``mon.a`` binds to ``1.2.3.4:1111``, and you want to add v2 on
port ``1112``,::

  ceph mon set-addrs a [v2:1.2.3.4:1112,v1:1.2.3.4:1111]

Once the monitors bind to v2, each daemon will start advertising a v2
address when it is next restarted.


.. _msgr2_ceph_conf:

Updating ceph.conf and mon_host
-------------------------------

Prior to Nautilus, a CLI user or daemon will normally discover the
monitors via the ``mon_host`` option in ``/etc/ceph/ceph.conf``.  The
syntax for this option has expanded starting with Nautilus to allow
support the new bracketed list format.  For example, an old line
like::

  mon_host = 10.0.0.1:6789,10.0.0.2:6789,10.0.0.3:6789

Can be changed to::

  mon_host = [v2:10.0.0.1:3300/0,v1:10.0.0.1:6789/0],[v2:10.0.0.2:3300/0,v1:10.0.0.2:6789/0],[v2:10.0.0.3:3300/0,v1:10.0.0.3:6789/0]

However, when default ports are used (``3300`` and ``6789``), they can
be omitted::

  mon_host = 10.0.0.1,10.0.0.2,10.0.0.3

Once v2 has been enabled on the monitors, ``ceph.conf`` may need to be
updated to either specify no ports (this is usually simplest), or
explicitly specify both the v2 and v1 addresses.  Note, however, that
the new bracketed syntax is only understood by Nautilus and later, so
do not make that change on hosts that have not yet had their ceph
packages upgraded.

When you are updating ``ceph.conf``, note the new ``ceph config
generate-minimal-conf`` command (which generates a barebones config
file with just enough information to reach the monitors) and the
``ceph config assimilate-conf`` (which moves config file options into
the monitors' configuration database) may be helpful.  For example,::

  # ceph config assimilate-conf < /etc/ceph/ceph.conf
  # ceph config generate-minimal-config > /etc/ceph/ceph.conf.new
  # cat /etc/ceph/ceph.conf.new
  # minimal ceph.conf for 0e5a806b-0ce5-4bc6-b949-aa6f68f5c2a3
  [global]
          fsid = 0e5a806b-0ce5-4bc6-b949-aa6f68f5c2a3
          mon_host = [v2:10.0.0.1:3300/0,v1:10.0.0.1:6789/0]
  # mv /etc/ceph/ceph.conf.new /etc/ceph/ceph.conf

Protocol
--------

For a detailed description of the v2 wire protocol, see :ref:`msgr2-protocol`.
