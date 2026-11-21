.. _msgr2:

Messenger v2
============

What is it
----------

The messenger v2 protocol, or msgr2, is the second major revision of
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

.. _address_formats:

Address formats
---------------

Prior to Nautilus, all network addresses were rendered like
``1.2.3.4:567/89012`` where there was an IP address, a port, and a
nonce to uniquely identify a client or daemon on the network.
Starting with Nautilus, we now have three different address types:

* **v2**: ``v2:1.2.3.4:578/89012`` identifies a daemon binding to a
  port speaking the new v2 protocol
* **v1**: ``v1:1.2.3.4:578/89012`` identifies a daemon binding to a
  port speaking the legacy v1 protocol.
* **TYPE_ANY**: ``1.2.3.4:0/123456`` identifies a client that can speak
  either version of the protocol. Prior to Nautilus, clients would appear as
  ``1.2.3.4:0/123456``, where the port of 0 indicates they are clients
  and do not accept incoming connections.  Starting with Nautilus,
  these clients are now internally represented by a **TYPE_ANY**
  address, and still shown with no prefix, because they may
  connect to daemons using the v2 or v1 protocol, depending on what
  protocol(s) the daemons are using.

Because daemons now bind to multiple ports, they are now described by
a vector of addresses instead of a single address.  For example, the
Monitor lines of ``ceph mon dump`` look like this::

  0: [v2:10.0.0.10:3300/0,v1:10.0.0.10:6789/0] mon.foo
  1: [v2:10.0.0.11:3300/0,v1:10.0.0.11:6789/0] mon.bar
  2: [v2:10.0.0.12:3300/0,v1:10.0.0.12:6789/0] mon.baz

The bracketed list or vector of addresses means that the same daemon can be
reached on multiple ports (and protocols).  Any client or other daemon
connecting to that daemon will use the v2 protocol (listed first) if
possible; otherwise it will fall back to the legacy v1 protocol.  Legacy
clients will only see the v1 addresses and will continue to connect as
they did before, with the v1 protocol.

The ``mon_host`` configuration option and the ``-m <mon-host>`` command
line option accept the same bracketed lists of addresses.


Bind configuration options
^^^^^^^^^^^^^^^^^^^^^^^^^^

Two new configuration options control whether the v1 and/or v2
protocol is used:

.. confval:: ms_bind_msgr1
.. confval:: ms_bind_msgr2

Similarly, two options control whether IPv4 and IPv6 addresses are used:

* :confval:`ms_bind_ipv4` [default: true] controls whether a daemon binds
  to an IPv4 address
* :confval:`ms_bind_ipv6` [default: false] controls whether a daemon binds
  to an IPv6 address

Connection modes
----------------

The v2 protocol supports two connection modes:

* *crc* mode provides:

  - a strong initial authentication when the connection is established
    (with CephX, mutual authentication of both parties with protection
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
    (with CephX, mutual authentication of both parties with protection
    from a man-in-the-middle or eavesdropper), and
  - full encryption of all post-authentication traffic, including a
    cryptographic integrity check.

  Secure mode uses `AES-128-GCM
  <https://en.wikipedia.org/wiki/Galois/Counter_Mode>`_,
  which is generally very fast on modern processors (e.g., faster than
  a SHA-256 cryptographic hash).

Connection mode configuration options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For most connections, there are options that control which modes are used:

.. confval:: ms_cluster_mode
.. confval:: ms_service_mode
.. confval:: ms_client_mode

There are a parallel set of options that apply specifically to
monitors, allowing administrators to set different (usually more
secure) requirements on communication with the monitors.

.. confval:: ms_mon_cluster_mode
.. confval:: ms_mon_service_mode
.. confval:: ms_mon_client_mode


Compression modes
-----------------

The v2 protocol supports two compression modes:

* *force* mode compresses messages of at least ``ms_osd_compress_min_size``
  bytes on connections between OSDs. It reduces that traffic at the cost of
  CPU time, which helps where the network between OSDs is slow or costly, for
  example between availability zones in a public cloud.

* *none* mode provides:

  - messages are transmitted without compression. 


Compression mode configuration options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For all connections, there is an option that controls compression usage in secure mode

.. confval:: ms_compress_secure

There is a parallel set of options that apply specifically to OSDs, 
allowing administrators to set different requirements on communication between OSDs.

.. confval:: ms_osd_compress_mode
.. confval:: ms_osd_compress_min_size
.. confval:: ms_osd_compression_algorithm

Transitioning from v1-only to v2-plus-v1
----------------------------------------

If ``ceph health`` reports ``MON_MSGR2_NOT_ENABLED``, one or more Monitors
have only a v1 address in the monitor map.

For most users, the monitors are binding to the default legacy port ``6789``
for the v1 protocol.  When this is the case, enabling v2 is as simple as:

.. prompt:: bash $

   ceph mon enable-msgr2

This adds a v2 address on port ``3300`` to each Monitor whose only address is
a v1 address on port ``6789``.

If the monitors are bound to non-standard ports, you will need to
specify an additional port for v2 explicitly.  For example, if your
monitor ``mon.a`` binds to ``1.2.3.4:1111``, and you want to add v2 on
port ``1112``:

.. prompt:: bash $

   ceph mon set-addrs a '[v2:1.2.3.4:1112,v1:1.2.3.4:1111]'


.. _msgr2_ceph_conf:

Updating ceph.conf and mon_host
-------------------------------

A CLI user or daemon normally discovers the Monitors via the ``mon_host``
option in ``/etc/ceph/ceph.conf``, which accepts bracketed lists of addresses.
For example, a line that lists only v1 addresses, like::

  mon_host = 10.0.0.1:6789,10.0.0.2:6789,10.0.0.3:6789

Can be changed to::

  mon_host = [v2:10.0.0.1:3300/0,v1:10.0.0.1:6789/0],[v2:10.0.0.2:3300/0,v1:10.0.0.2:6789/0],[v2:10.0.0.3:3300/0,v1:10.0.0.3:6789/0]

However, when default ports are used (``3300`` and ``6789``), they can
be omitted::

  mon_host = 10.0.0.1,10.0.0.2,10.0.0.3

Once v2 has been enabled on the Monitors, ``ceph.conf`` may need to be
updated to either specify no ports (this is usually simplest), or
explicitly specify both the v2 and v1 addresses.

When you are updating ``ceph.conf``, note the new ``ceph config
generate-minimal-conf`` command (which generates a barebones config
file with just enough information to reach the Monitors) and the
``ceph config assimilate-conf`` (which moves config file options into
the Monitors' configuration database) may be helpful.  For example:

.. prompt:: bash #

   ceph config assimilate-conf -i /etc/ceph/ceph.conf -o /etc/ceph/ceph.conf.unassimilated
   ceph config generate-minimal-conf > /etc/ceph/ceph.conf.new
   cat /etc/ceph/ceph.conf.new

::

  # minimal ceph.conf for 0e5a806b-0ce5-4bc6-b949-aa6f68f5c2a3
  [global]
          fsid = 0e5a806b-0ce5-4bc6-b949-aa6f68f5c2a3
          mon_host = [v2:10.0.0.1:3300/0,v1:10.0.0.1:6789/0]

Before you replace the file, read ``/etc/ceph/ceph.conf.unassimilated``: it
lists the options that were not imported into the Monitor configuration
database. The old file's ``fsid`` and ``mon_host`` lines always appear in it,
because the Monitors do not store these options, and the new file already has
both. Add to the new file any other options that the host still needs.

.. prompt:: bash #

   mv /etc/ceph/ceph.conf.new /etc/ceph/ceph.conf

Protocol
--------

For a detailed description of the v2 wire protocol, see :ref:`msgr2-protocol`.
