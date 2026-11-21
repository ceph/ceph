==================================================
 Extended Features and Clean Rejection of Clients
==================================================

Summary
=======

Ceph is running out of connection feature bits, and a client that lacks a
required feature retries until it times out instead of failing. This design
gives Ceph a set of feature numbers with no fixed limit, for clients and
daemons. It also makes a client that the cluster turns away fail at once, with
a reason. Both target the W release; Vampire gets a first step that needs no
wire change (see "Phasing").

- **No fixed limit on feature numbers.** One unused msgr2 banner bit lets a
  peer append its 64-bit feature word and a variable-length set of extended
  features to the banner it already sends. Peers that don't set the bit see
  nothing new, and a new client that connects to an older cluster is admitted
  as today.
- **A clean "no".** The banner arrives before authentication, so the mon knows
  what a client supports when it authenticates it. It turns away a client that
  lacks a required feature there. Every connecting client, old or new, kernel
  or userspace, already treats a refusal at that point as final.
- **Requirements gate new connections only.** A client that is already
  connected keeps working until it reconnects. A client from before W that is
  refused then never tries again, and its I/O hangs once its tickets expire,
  even if the requirement is lifted. So turning a requirement on must first
  check the connected clients, as ``set-require-min-compat-client`` does.
- **Kernels stay simple.** Unmodified kernels need nothing and are refused
  cleanly. To take part, a kernel needs 12 lines of code.
- **Phased.** Vampire gets ``ceph osd set-require-min-compat-client`` accepting
  feature names, and the MonClient fix; neither changes the wire. The extended
  set and the clean rejection follow in W.

.. note::

   **Why a clean rejection is needed.** Some features change where clients must
   send their I/O. While a pool is being migrated, for example, a client that
   cannot follow the migration would read and write the wrong pool and corrupt
   data. So while such a feature is in use, clients that lack it must be kept
   out, and Ceph already keeps them out: the OSDMap says which features the
   cluster's current state requires (``OSDMap::get_features``), and the mons
   refuse clients that lack them when they connect. What is wrong today is how
   they are refused. The client cannot tell the refusal from a network fault:
   it retries for minutes to hours, librados can even report success, and
   operators see nothing at default debug levels. This design keeps the same
   decision and makes the refusal immediate, final and explained.

   **This design does not reject old clients in general.** A client is refused
   only for a feature that an operator has explicitly required, with a command
   that first checks the clients connected at the time, as ``ceph osd
   set-require-min-compat-client`` does today. The cluster refuses to use such
   a feature, to start a pool migration for example, until the operator has
   required it. A cluster whose operator requires nothing admits every client
   it admits today. The design adds no requirement of its own: in the draft,
   ``ceph osd require-client-features`` is the only way to set one, besides a
   debug option for tests.

The problem
===========

Main has five unadvertised connection bits left. At the rate since 2017, about
two bits per release, that lasts two releases.

- **Every bit since 2018 has been a reused retired bit.** 11 of the last 21
  were ``SERVER_<release>`` markers, one per release.
- **Reuse is fragile for client-facing bits.** A reused bit is really a set:
  the bit plus a marker bit that no release with the old meaning ever sent,
  tested as ``(features & set) == set``. Choose the marker wrong and old
  releases pass: Pacific OSDs look like Reef (the v18.2.8 known issue), and
  Luminous to Nautilus clients, which send incarnation bit 57 together with old
  meanings of bits 52 and 54, pass the ``CRUSH_MSR`` requirement (confirmed
  live). Pairing with a later marker such as ``SERVER_OCTOPUS`` fixes each
  case, but every new bit needs that care, and some messenger interfaces still
  test ``(features & flag) != 0``.

Requirements
============

An extension must keep everything a connection feature bit gives us today, and
add one thing bits never gave us: a clean "no".

.. list-table::
   :header-rows: 1

   * - ID
     - Requirement
     - How connection bits do it today
     - What the extension must do
   * - R1
     - Negotiate at connect
     - 64-bit supported and required masks in ``CLIENT_IDENT`` and
       ``SERVER_IDENT``
     - Each end learns the other's set before the session starts, once per
       connection
   * - R2
     - Enforce at connect
     - ``OSDMap::get_features`` sets ``Policy::features_required``; the server
       answers ``IDENT_MISSING_FEATURES``
     - The same policies can require extended features, checked at the same
       point
   * - R3
     - Show who has what
     - ``MonSession``, ``FeatureMap``, ``ceph features``, health checks
     - The same, including peers that have no extended features at all
   * - R4
     - Choose encodings
     - ``encode(..., features)`` picks struct versions
     - Not needed in the first version
   * - R5
     - Reject cleanly
     - The client retries until its mount timeout
     - A client turned away for good gets an error and a reason, and stops
   * - R6
     - Change nothing for others
     - n/a
     - Kernel clients, crimson, msgr1 and every older release work as before;
       unknown bits are ignored; a number is never reused

Prior art
=========

Sage proposed this in 2018: "add a second set of 64 feature bits everywhere.
Maybe that is something we should build into msgr2 regardless" (`PR #22097
<https://github.com/ceph/ceph/pull/22097>`__). Ilya agreed that the msgr2
handshake should have "enough space for feature bits to last us a lifetime".
msgr2 shipped in Nautilus with one 64-bit word, and nobody followed it up.

.. list-table::
   :header-rows: 1

   * - When
     - Who
     - What
     - Why it matters here
   * - 2026-03
     - Patrick, Radek
     - Pacific OSDs look like Reef: ``SERVER_REEF`` reused a bit that Pacific
       still sends (`dev@
       <https://lists.ceph.io/hyperkitty/list/dev@ceph.io/message/Q2PE2AFBRWN7BFKKG247DBSLW3ZEXZJQ/>`__)
     - Bit reuse fails in the field
   * - 2025-09 to 2026-04
     - Sam, Patrick
     - NVMe-oF beacon bit removed in favour of ``mon_feature_t`` (`PR #65563
       <https://github.com/ceph/ceph/pull/65563>`__)
     - Patrick: "this connection feature bit should not have been added to
       begin with"
   * - 2023
     - Greg, Ilya
     - Client-visible bits cannot be retired; assume a lifetime of at least ten
       years (`PR #51488 <https://github.com/ceph/ceph/pull/51488>`__)
     - Client-facing features need a namespace without reuse
   * - 2021-03
     - Ilya
     - Requiring ``CEPHX_V2`` gives old clients ``ENOTSUP`` "instead of
       spinning on 'feature set mismatch' faults" (`PR #40404
       <https://github.com/ceph/ceph/pull/40404>`__)
     - An authentication error is a clean rejection
   * - 2021-02
     - Sage
     - On-wire compression gated by "a msgr2 feature instead of a ceph feature
       bit" (`dev@
       <https://lists.ceph.io/hyperkitty/list/dev@ceph.io/message/3EPAOTQGDDRAUC6W7TKEKQYV4UK5LRKI/>`__)
     - msgr2 has its own 64 bits; 2 are used
   * - 2018-11
     - Sage, Ilya
     - A second set of 64 bits in msgr2 (`PR #22097
       <https://github.com/ceph/ceph/pull/22097>`__)
     - This proposal, never built
   * - 2018-05
     - Zheng, Patrick
     - CephFS moves to a variable-length ``feature_bitset_t`` and rejects or
       evicts old sessions
     - Same idea for one service
   * - 2017-02
     - Sage
     - Incarnations: reuse retired bits behind a marker bit (`PR #13278
       <https://github.com/ceph/ceph/pull/13278>`__)
     - Designed for server-side bits only
   * - 2016
     - Joao
     - ``mon_feature_t`` in the MonMap (`PR #7556
       <https://github.com/ceph/ceph/pull/7556>`__)
     - Mon-only namespace

Other protocols point the same way:

- **Never reuse a number, and keep the space large enough that you never need
  to** (Kafka, protobuf, NFSv4). Incarnations are the thing to avoid.
- **Send random unknown values so that "ignore what you don't know" stays
  true** (TLS GREASE, `RFC 8701 <https://www.rfc-editor.org/rfc/rfc8701>`__).
  Unused tolerance rots: Ceph's control frames already ignore trailing bytes,
  but nothing has ever relied on it.
- **Authenticate the negotiation** (TLS 1.3, SMB 3.1.1, and the SSH Terrapin
  fix). msgr2 already signs every byte sent before authentication, including
  the banner, whenever cephx is in use.
- **Say no with something the old peer already treats as final** (Kafka, ONC
  RPC). For old Ceph clients that is an authentication error at the mon, not a
  feature mismatch.

Options considered
==================

Only a set carried in msgr2 meets all six requirements. Every other option
either runs out, or cannot tell the servers what a client has.

.. list-table::
   :header-rows: 1

   * - Option
     - Verdict
     - Why
   * - Keep reusing retired connection bits
     - Rejected for anything clients see
     - Five are left. Old releases already pass ``HAVE_FEATURE`` for some
       reused bits, and client bits cannot be retired.
   * - An OSD session hello, like CephFS
     - Rejected
     - Every client would need a new handshake with every OSD, and every route
       into a pool would need its own guard. An old server faults the
       connection on the unknown message type, so new clients would loop
       against it.
   * - A feature set in the OSDMap
     - Half of the answer
     - It can say what a pool requires, but the map flows from servers to
       clients. Servers still cannot learn what a client has, and old clients
       skip unknown fields by design.
   * - A second fixed 64-bit word
     - Rejected
     - It is the same work as a variable set and runs out again.
   * - **A variable-length set in msgr2**
     - **Chosen**
     - Negotiated by an unused msgr2 banner bit (2 of 64 are used), carried in
       the banner before authentication, and invisible to every peer that does
       not offer it. Sage proposed it in 2018.

The chosen design keeps the OSDMap idea for one thing: where a pool requires a
feature (pool migration, pools over zones), the requirement is computed from
the map, as ``OSDMap::get_features`` does today.

Where in msgr2
--------------

msgr2 already has 62 unused banner bits, but using them as RADOS features is
worse than appending a set to the banner.

.. list-table::
   :header-rows: 1

   * - Placement
     - Verdict
     - Why
   * - Banner bits 8 to 62 as RADOS features
     - Rejected
     - It gives 55 bits that can never be reused, so it runs out again. The
       64-bit feature word still arrives only after authentication, so the mon
       cannot refuse a client for it there. It also mixes protocol features
       with RADOS features.
   * - New frames after authentication
     - Rejected
     - A refusal after ``AUTH_DONE`` bypasses the hunting backoff of MonClient
       and of the kernel. A prototype caused a reconnect storm of about 300
       connections a second. The kernel patch is about 114 lines.
   * - New fields at the end of the identification frames
     - Rejected
     - These also arrive after authentication, so mounted kernels have the same
       reconnect gap. The kernel patch is about 60 lines, including changes to
       ``mon_client.c``.
   * - **A set appended to the banner**
     - **Chosen**
     - It arrives before authentication, adds no state per connection, and
       needs 12 lines of kernel code, all in ``prepare_banner()``.

Design: extended features in the msgr2 banner
=============================================

One unused msgr2 banner bit, ``EXTENDED_FEATURES`` (bit 2), switches the
extension on. A peer that sets it appends its 64-bit feature word and a
variable-length extended set to the banner it already sends. Nothing else on
the wire changes: there are no new frame types, and the identification frames
keep their layout.

::

   "ceph v2\n"  __le16 payload_len
   __le64 msgr2_supported    bit 2 = EXTENDED_FEATURES (never set in required)
   __le64 msgr2_required
   -- only if bit 2 is set --
   __le64 features           the CEPH_FEATURE_* word it sends in CLIENT_IDENT
                             or SERVER_IDENT
   __le32 ext_len            bytes, a multiple of 8, at most 512
   __le64 ext[ext_len / 8]   extended feature n = bit n % 64 of word n / 64
   ...                       later fields; receivers ignore what they don't
                             know

**Why the banner.** The banner arrives before authentication, so the mon knows
exactly what a client supports when it authenticates it. That gives one refusal
point for every connecting client, old or new, kernel or userspace, through a
path they all already treat as final. It also keeps the kernel change to 12
lines of code, all in ``prepare_banner()``. With cephx, the authentication
signature covers the banner in both crc and secure mode, so the extended part
cannot be stripped unnoticed.

**Rules.**

- **Numbers.** Extended features are numbered from 0 with no fixed limit,
  allocated upstream in order, and never reused or renumbered. The wire format
  carries any number of 64-bit words. Each release keeps at most 64 of them
  (4,096 numbers, ``feature_set_t::max_blocks``) and skips the rest, so that a
  peer cannot make a daemon allocate whatever it claims before authentication.
  A number past the receiver's cap counts as absent, which is the right answer
  for a feature it does not know, and a later release raises the cap without a
  wire change. A downstream backport reserves its number upstream first, which
  costs nothing now.
- **Room in the banner.** The payload length is 16 bits, so a banner is at most
  64 KiB, shared by everything in it. The set is length-prefixed, and a sender
  never emits more than 64 words, so the extension takes about 524 bytes at
  most and a reader steps over it to whatever follows. Later additions go after
  the set, switched on by one of the 61 unused banner bits or by an extended
  feature number.
- **Unknown bits.** Receivers ignore unknown bits and unknown trailing bytes.
  The top bit of every 64-bit word is reserved: with
  ``ms_v2_ext_features_grease`` a peer sets one at random ("GREASE"), so a peer
  that mishandles unknown bits is caught early. The msgr tests always turn it
  on.
- **Requirements.** They stay per peer type in the messenger ``Policy``
  (``features_ext_required`` plus a reason text), and are computed from cluster
  state as ``OSDMap::get_features`` does today. A requirement becomes active
  only once every mon supports it. Only mons in the quorum refuse at
  authentication: a mon outside it may hold an older policy, and clients stop
  hunting at the first refusal.
- **One feature word.** The word in the identification frame must equal the one
  in the banner. A peer that sees them differ drops the connection, which makes
  the word tamper-evident in crc mode.
- **Visibility.** ``ceph features extended`` and the mon session dump show each
  client's set, and whether it can exchange one at all.
- **Release markers.** About half of all new connection bits since 2017 were
  ``SERVER_<release>`` markers. Future releases should put them in the extended
  set, or better, stop needing them: a release marker claims everything in that
  release, which is why the kernel client still reports Luminous and why its
  maintainers hesitate to move it (every behaviour the marker implies must be
  checked first). An extended number claims one feature, so a kernel can add
  exactly what it implements.

**Costs, accepted.**

- Every peer now receives the extra banner bytes. Every released peer tested
  ignores them.
- The banner travels in clear text before authentication, even in secure mode,
  so it reveals the client's feature set.
- The 64-bit word is sent twice. The handshake grows by 20 bytes each way, with
  no extra frame or round trip.

Rejecting clients cleanly
=========================

The mon turns clients away during authentication, the one point where every
connecting client, old or new, kernel or userspace, already treats a refusal as
final. Today a client that lacks a required feature is refused at
identification instead. It retries until ``client_mount_timeout``, and over
msgr2 it can retry for much longer because of the false success described
below.

.. list-table::
   :header-rows: 1

   * - Client
     - How the mon refuses it
     - What the client shows
   * - W or later userspace
     - ``AUTH_BAD_METHOD(-EPROTONOSUPPORT)`` plus the reason trailer
     - The error and the mon's reason
   * - Patched kernel, msgr2
     - The same
     - dmesg: ``refused session: <reason>``
   * - Unpatched kernel, msgr2
     - ``AUTH_BAD_METHOD(-EPROTONOSUPPORT)``
     - ``rbd map``: ``EACCES``; CephFS: ``EHOSTUNREACH``; dmesg shows -93
   * - Any client over msgr1 (kernels by default, Luminous, Mimic)
     - ``MAuthReply(-EPROTONOSUPPORT)``, naming the auth protocol (but never
       ``none``, whose clients take any reply as success), with the reason in
       ``result_msg``; the connection stays open so that the client reads the
       reply
     - errno 93; unpatched kernels log "mauth authentication failed: -93" (5.4:
       "auth method 'x' error -93"); a patched kernel prints the reason
   * - Older userspace over msgr2, missing an extended feature
     - Refused at authentication, since its banner has no bit 2
     - "Permission denied"; the mon logs the reason
   * - Older userspace over msgr2, missing a 64-bit feature
     - Two strikes: identification is refused once, then its next
       authentication
     - "Permission denied"
   * - W or later clients already connected
     - Not refused until they reconnect. Then they retry every 3 to 30 s
     - They recover if the requirement is lifted within one to three times
       ``auth_mon_ticket_ttl`` (12 hours by default). After that their old
       ticket's secret is gone, the mons refuse them with ``EACCES``, and only
       a restart helps
   * - Older clients already connected
     - Not refused until they reconnect. Then they never try again
     - I/O goes on over their open OSD connections. A new one, after an OSD
       restart for example, needs a fresh ticket; once theirs expires (one to
       three times ``auth_service_ticket_ttl``, an hour by default), their I/O
       hangs. Lifting the requirement does not help; only a restart does

**Two strikes, for clients we cannot classify at authentication.** An older
msgr2 client sends only the old 64-bit word, and only after authentication. So
the first refusal happens at identification, as today, and the mon remembers
the client's ``global_id`` for 60 seconds. The client reconnects within a
second with the same ``global_id``, and that authentication is refused. With
cephx the ``global_id`` is not yet verified at that point, so a forged one only
turns away the connection presenting it. The mon keeps each entry until it
expires, so a forger cannot clear one either.

**A requirement at the mon applies to the whole cluster.** The mon knows
nothing of the pools a client will use, so while it requires a feature, every
new client without it is refused, whatever its pools: every new kernel mount,
``rbd map``, ceph-csi pod and gateway restart. A need that concerns one pool,
such as an active pool migration, is better enforced by the OSDs for operations
on that pool, with ``-EPROTONOSUPPORT``; this design does not provide that.

**Mon and OSDs both police it; the window is elsewhere.** The mon refuses every
new client connection, and the OSDs refuse 64-bit mismatches at identification
today through ``OSDMap::get_features``; they should check the banner's extended
set the same way, which is cheap since every peer exchanges it. Neither closes
the real window: clients that were connected before the requirement started,
which keep their OSD connections and hang only later (see "Turning a
requirement on"). That is why the requirement is turned on explicitly, with a
check of connected clients, rather than by using the feature. Never use
``-EOPNOTSUPP``: librbd and rgw take it as "fall back", and kernels print a
warning with a stack trace when they see it at authentication.

**MonClient changes.** The first two fix bugs that exist since Nautilus.

- **No false success.** Over msgr2, a client refused at identification can get
  ``rados_connect()`` returning +1, which C callers take as success. It then
  retries for 11 to 21 times ``client_mount_timeout`` (55 minutes to nearly two
  hours with the default 300 s) and then reports "Operation not permitted".
  ``authenticate()`` must never report success when the session was refused.
- **Recovery.** A running client whose re-authentication fails gives up
  silently and never tries again. When the mons turn it away for missing
  features, it should keep retrying, backing off from 3 to 30 s, as the kernel
  does. After any other auth error, such as a deleted key, it stops as before,
  saying so once: retrying those would only fill the mons' logs.
- **Stop hunting at the first refusal.** A refused client keeps hunting while
  its connection to another mon is still pending, for example to a mon that is
  down, and fails only at ``client_mount_timeout``, with a timeout error. The
  refusal reflects quorum state that every mon in the quorum shares, and mons
  outside it do not refuse, so the client should stop at the first one.
  Released clients keep the old behaviour, so while a mon in their list is down
  they wait out ``client_mount_timeout``.

**Operators see why.** The mon writes "turned away N connection attempt(s) from
clients lacking required features (latest at <addr>): <reason>" to the cluster
log, at most once a minute. Each attempt is logged only at ``debug_mon`` 10:
kernels retry forever, and at the default level each refusal would add a line
of about 300 bytes to the mon log. Today these clients are invisible at default
debug levels. A health check should name them too.

**This part needs no new wire format.** It covers refusing at authentication
for 64-bit features, the two-strike rule, the msgr1 refusal and the MonClient
changes. With it, old clients that connect while a pool migrates are turned
away in about a second, and clients that support pool migration keep working
throughout. It is W work all the same, except the first MonClient fix, which
Vampire gets (see "Phasing").

Turning a requirement on
========================

Requirements gate new connections only, so turning one on is the moment that
decides what happens to the clients already connected. In a test with short
tickets, three clients from before the change (one patched, main's, and a
released Reef) were refused when the mons restarted, kept writing for 7
minutes, and then hung for good when an OSD restarted. Lifting the requirement
brought none of them back.

- **An operator turns a requirement on, explicitly and once.** A requirement is
  never derived from what the cluster happens to be doing: a feature that needs
  one cannot be used, and a pool migration cannot start, until an operator has
  required it. Once required, it stays required, so the moment that decides the
  fate of connected clients comes once, while the operator is watching, instead
  of at every migration, unattended. The draft adds ``ceph osd
  require-client-features <feature>...`` and ``unrequire-client-features`` as a
  stand-in. The command to keep is ``ceph osd set-require-min-compat-client``:
  Vampire teaches it feature names, and W extended features (see "Phasing").
- **Check the connected clients first.** The command refuses while clients
  connected to the quorum lack the feature, unless forced, as
  ``set-require-min-compat-client`` does for a release. It counts the clients
  that cannot exchange extended features at all (done in the draft). Note what
  that command does today: it checks the clients connected when it runs, and
  afterwards refuses map changes (tunables, upmap) that would need newer
  clients. It does not by itself refuse later connections; those are refused
  only for features the map's state implies, through ``OSDMap::get_features``.
  A requirement set by the extended command must do both.
- **When forced, make the failure visible.** Blocklisting the clients that lack
  the feature makes them fail at once, with an error, instead of hanging hours
  later.
- **Every client counts.** That includes the ``ceph`` CLI from an older
  package, the Rook operator, whose CLI comes from an older Ceph image, the
  librados handles of an older mgr's modules, gateways, and OpenStack or
  Proxmox hosts with a distribution's librbd. An older CLI sees only
  "Permission denied" and cannot read the cluster log that explains why.
- **Leave an escape.** Today the escape is ``ceph daemon mon.<id> config set
  ...`` on every mon, or a CLI from the cluster's image. A requirement kept in
  the OSDMap will need a mon admin socket command that suspends client
  requirements.
- **Never automatically.** Not from ``require_osd_release``, which Rook sets by
  itself. Gate on the monmap's ``min_mon_release`` and on the mgr versions too.

Compatibility and upgrades
==========================

Nothing changes for a peer that does not set bit 2. Everything below follows
from that.

.. list-table::
   :header-rows: 1

   * - Peer
     - What it sees
     - What it must do
   * - Kernel clients today
     - A longer banner, which they ignore (5.11 to master parse it the same
       way). They count as having no extended features.
     - Nothing, unless the cluster requires an extended feature. Then a mount
       or map fails at once at the mon. The refusal tells users to mount with
       ``ms_mode=prefer-crc`` and use a newer kernel.
   * - Future kernels
     - n/a
     - Patch 1 (12 lines of code in ``prepare_banner()``) to take part. Patches
       2 and 3 only log, so that dmesg shows the mon's reason.
   * - msgr1 peers (Luminous and Mimic, and every kernel by default)
     - No banner, so no extended features
     - msgr1 cannot carry them. ``rbd device map`` has defaulted to msgr2 since
       Tentacle; ``mount.ceph`` uses it with the new syntax.
   * - crimson
     - Its own msgr2 mask, without bit 2
     - No extended features until it opts in (about 60 to 90 lines).
   * - Older userspace releases
     - A longer banner, which they ignore
     - Nothing, unless something is required: then they are refused at
       authentication in under a second.
   * - New clients and daemons against an older cluster
     - Older mons and OSDs ignore bit 2 and the trailer, and set no bit 2
       themselves
     - Nothing. The client marks the peer as unable to exchange extended
       features and the connection runs as today. Tested with released mons
       from Nautilus to Umbrella and with main's mon and OSD, in both
       directions.
   * - Older mons in a mixed quorum
     - Nothing new
     - A requirement must wait until every mon is W or later, or an older mon
       would admit the clients that a newer one refuses.

**msgr2 is required only where an extended feature is.** msgr1 clients keep
working while the cluster requires no extended feature of clients; requirements
on the 64-bit word still work over msgr1. Once a feature that uses an extended
number is in use, every client must be new enough to have that feature, and
must use msgr2.

**msgr1 cannot be removed in W, but deprecation can start.** Every kernel
checked still defaults to msgr1, and no upstream decision exists. Steps that W
can take:

- make the mon honour ``ms_bind_msgr1`` (`tracker 70457
  <https://tracker.ceph.com/issues/70457>`__ is open);
- warn about msgr1 sessions;
- create new cephadm clusters without msgr1, as Rook 1.21 already does;
- ask the kernel client maintainers to change the default to ``prefer-crc``, as
  planned in 2020.

**cephx shapes the story in three ways.**

- **Tamper protection.** The banner, and so the extended set, is tamper-proof
  only when cephx is used. With auth ``none`` nothing on the wire is protected,
  as today.
- **New clusters already exclude old clients.** A new Vampire cluster allows
  only AES-256 keys. Clients older than Squid 19.2.6, Tentacle 20.2.4 or
  Umbrella 21.1.1, and kernels without AES-256 support such as 5.4, are already
  refused at authentication, with misleading messages, unless an operator
  allows the old cipher.
- **Not access control.** A client refused for a missing feature at
  identification has already completed cephx and holds tickets. Feature
  requirements are compatibility gates; cephx caps remain the access control.

Security
========

The extended set is exchanged before authentication, so a peer that has not
proved who it is can send one. Everything a peer can send before authentication
is bounded, and the bounds are no wider than main's today.

- **The banner is at most 64 KiB.** Its payload length is a 16-bit field that
  already exists on main; every peer already reads a payload of that length
  from any connection. This design only uses bytes within that bound.
- **The extended set keeps at most 64 words (512 bytes).**
  ``feature_set_t::decode`` allocates that much whatever length the peer
  claims, and skips the rest. A length that is not a multiple of 8, or longer
  than the data, is a decoding error and the connection faults. The unit tests
  cover these cases, including a fuzz test.
- **The reason text travels from server to client only.** A client truncates it
  to 512 printable characters before logging or storing it. A client never
  sends one.
- **The two-strike table** on the mon holds one ``global_id`` per refused
  attempt for 60 seconds, so it grows with the refusal rate and no further,
  like the session table beside it.
- **Work per refusal** is a comparison of at most 64 words and a cluster-log
  line at most once a minute, the same order as today's refusal at
  identification.
- **The negotiation is authenticated** when cephx is in use: every byte sent
  before ``AUTH_DONE``, the banner included, is signed, so a set that is
  changed in flight is detected and the connection dropped. With auth ``none``
  nothing on the wire is protected, as today. The set is readable before
  authentication, even in secure mode; it reveals which features a client has
  and nothing else.

Pre-authentication frame sizes on main (``HELLO``, ``AUTH_REQUEST``) are
outside this design and are not changed by it.

**An attacker on the path** can alter the banner or inject a refusal.

- A banner *inflated* to claim a feature the client lacks is detected at
  ``AUTH_DONE`` when cephx is in use, because the banner is signed, and the
  connection is dropped: a lacking client is never silently admitted, which is
  the outcome that could corrupt data. With auth ``none`` the inflation goes
  unnoticed, as everything does under auth ``none``.
- A banner *stripped* of a feature gets the client refused before
  ``AUTH_DONE``, so the signature is never checked and the tampering is not
  detected. The outcome is only a refusal.
- A forged ``AUTH_BAD_METHOD(-EPROTONOSUPPORT)`` makes a client stop hunting at
  once and a running client back off. The same attacker can forge ``-EACCES``,
  which clients already treat as final, or reset the connection, so this is not
  a new class of attack; it can never gain admission, which still needs the
  key.
- A forged or rogue mon can send any reason text. Userspace clients truncate it
  to 512 printable characters before logging or storing it. The kernel patches
  that print it to dmesg must cap its length and filter it to printable
  characters in the same way.

**What this design makes worse, and the mitigations.**

- **A new parser runs before authentication.** ``feature_set_t::decode`` is
  reachable by anyone who can connect. It is 15 lines, allocates a bounded
  amount, and has a fuzz test; it must stay that small, and belong to any msgr2
  fuzzing corpus.
- **One refusal stops the hunt.** Today a client that one mon turns away keeps
  trying the others; with the design it stops at the first
  ``-EPROTONOSUPPORT``, so a single rogue or misconfigured mon address in a
  client's list can stop a hunt that would have found a good mon. Only quorum
  mons refuse, which handles the misconfigured case but not a rogue. Mitigation
  to adopt: treat a refusal as final only once every mon tried in the current
  round has refused or failed. When the quorum agrees this costs nothing;
  against one rogue it costs at most a connection timeout (10 s), never
  ``client_mount_timeout``.
- **A refused msgr1 connection stays open.** The mon used to close the
  connection straight after its reply; the design leaves it open so that the
  client reads the reply, and today it is reaped only by
  ``ms_connection_idle_timeout`` (15 minutes). An unauthenticated peer can
  therefore hold a mon fd and session for that long per refused connection.
  Mitigation to adopt: close the connection a few seconds after the reply, by
  timer, as the draft already does nothing else with it.
- **Refusals reach the cluster log and, later, a health check.**
  Unauthenticated peers can cause both. The cluster-log line is rate-limited to
  one a minute and carries only counts and an address; the health check must do
  the same and never include a string a peer chose.


Risks and open questions
========================

No risk found blocks the design. Most are rules to write down and keep testing.

.. list-table::
   :header-rows: 1

   * - Risk
     - Mitigation
   * - A future parser rejects longer banners. Kernel hardening in 2026 chose
       "at least 16 bytes"; "exactly 16" would break us.
     - Write "receivers ignore unknown trailing bytes" into ``msgr2.rst``, tell
       the kernel client maintainers before the kernel series, and keep GREASE
       on in tests.
   * - Crimson and third-party msgr2 code meet the longer banner. No
       third-party implementation is known.
     - Done: a crimson OSD works in a cluster whose mons and clients send the
       longer banner with GREASE (see the testing section). Run it again before
       merging.
   * - Refusing after authentication succeeded makes clients reconnect without
       backoff.
     - Refuse before ``AUTH_DONE``, at the mon, as this design does. Refused
       running clients back off from 3 to 30 s.
   * - In a mixed-version quorum, an older mon admits what a newer one refuses.
     - A requirement becomes active only when every mon is W or later.
   * - Requirements apply only to new connections, and clients connected when
       one starts hang later.
     - Check connected clients before turning a requirement on, and blocklist
       those that lack it when forced (see above).
   * - An operator's older CLI is locked out with "Permission denied".
     - An escape through each mon's admin socket; document the symptom.
   * - A mon outside the quorum, with an older policy, ends clients' hunt. In a
       test, 30 of 30 clients failed.
     - Only mons in the quorum refuse at authentication.
   * - A flood of refused kernels fills the mon log.
     - Each refusal at ``debug_mon`` 10; one cluster-log line a minute.
   * - Auth ``none`` gives no integrity, so a man in the middle can strip bit 2
       silently.
     - As today; document it. With cephx the strip is detected.
   * - The banner is readable before authentication, even in secure mode, so it
       reveals the feature set.
     - Accepted; it reveals no secrets.
   * - Feature numbers cannot be reused, and a release keeps at most 4,096 of
       them.
     - Allocate upstream in order, and raise the cap in a later release, which
       changes no wire format; the type ignores numbers past the limit, and
       options reject them, and GREASE bits, with a logged error.

**Open questions.**

- Should future releases put their ``SERVER_<release>`` markers in the extended
  set, or send a release number?
- Should a downstream product get its own range of numbers, or always reserve
  upstream first? Reserving upstream is cheap once numbers have no fixed limit.
- What should the health check that names refused clients say?

Phasing
=======

The work is split by release. Vampire gets what pool migration needs without a
wire change; the extended set and the clean rejection get the discussion they
need in W.

- **Vampire: require 64-bit features by name.** ``ceph osd
  set-require-min-compat-client`` accepts feature names as well as a release.
  The OSDMap carries the explicit requirement, and ``OSDMap::get_features``
  folds it in, so the mon and OSD checks that enforce release-derived bits
  today enforce it too. A name whose bit needs a marker is a set:
  ``pool_migration`` means bit 54 with ``SERVER_OCTOPUS``. The command checks
  connected clients first, as it does today. Without the clean rejection, a
  refused client fails as it does today: kernels retry for about two minutes
  and userspace clients for 3.7 minutes. The first MonClient fix (step 1 below)
  goes to Vampire too, so that userspace clients end with an error rather than
  a false success. Pool migration does not depend on this for safety, since it
  refuses clients from the map while a migration runs; the command lets an
  operator require the feature ahead of time.
- **Kernel clients and bit 54.** The cluster takes a client as able to follow a
  migration only if it sends bit 54 and ``SERVER_OCTOPUS``. The kernel sends
  neither, so it is refused while a migration runs. To pass, it must follow
  migrations: read the migration state from the pool, check each I/O against
  the watermark and redirect it, and claim ``SERVER_OCTOPUS``, a release marker
  whose every implication its maintainers will want audited. The feature bits
  are one line; the behaviour is the work. The kernel needs no change to be
  refused, cleanly once W's server side is in.
- **W: the rest of this document.** The extended set, the clean rejection, the
  two-strike rule, GREASE, and ``set-require-min-compat-client`` extended once
  more, to extended features.

Implementation plan
===================

There are five steps, in order. Step 1 and the Vampire command (see "Phasing")
change no wire format and go first; steps 2 to 5 are W work; only the fourth
needs anyone outside Ceph's userspace.

#. **MonClient fix.** Make ``authenticate()`` never report success when the
   session was refused. A small, separate pull request, backported to every
   supported release: `PR #73638 <https://github.com/ceph/ceph/pull/73638>`__
   (`tracker #82890 <https://tracker.ceph.com/issues/82890>`__).
#. **Refuse clients at authentication for missing 64-bit features.** This
   covers the two-strike rule for older msgr2 clients, the msgr1 refusal in
   ``MAuth`` with the reason in ``result_msg``, the cluster-log line and a
   health check. MonClient stops hunting at the first such refusal, and a
   running client turned away keeps retrying with backoff. No wire change.
#. **Extended features in the banner, for W.** It contains:

   - the ``msgr2.rst`` spec;
   - the feature-set type and the list of feature numbers;
   - the banner and reason trailers in ``ProtocolV2``;
   - requirements in the messenger ``Policy``;
   - mon refusal using the banner;
   - ``ceph features extended``;
   - tests: the msgr suite with GREASE and a per-connection check, and the
     feature-set unit tests.

   Crimson can follow later (about 60 to 90 lines).
#. **Kernel series,** after step 3 merges. Three patches for the kernel client:
   12 lines of code to take part, plus two that only log the mon's reason.
#. **Use it.** From W on, new client-facing features and release markers take
   extended numbers, not connection bits. Start deprecating msgr1: make the mon
   honour ``ms_bind_msgr1``, warn about msgr1 sessions, create new clusters
   without msgr1, and ask for the kernel default to change.

Testing so far (temporary)
==========================

.. note:: This section records what a prototype of this design has been
   tested with, for review. It will be removed before this document is
   merged.

The draft implementation, `PR #73605
<https://github.com/ceph/ceph/pull/73605>`__, implements steps 1 to 3; its step
1 is also `PR #73638 <https://github.com/ceph/ceph/pull/73638>`__. The rows
from "Clients connected when a requirement starts" to "Mon log volume" ran on
the draft itself; the others ran on the prototype it was cleaned up from. Every
experiment used real binaries: the prototype or the draft, upstream main,
released clients from Luminous 12.2.13 to Umbrella 21.2.0, and kernels 5.4,
6.12 and 7.3 booted in VMs, with and without the kernel patch.

- Every released client tested from Nautilus 14.2.22 to Umbrella 21.2.0, and
  every kernel tested, ignores the longer banner.
- Every kernel and userspace client is refused in 0.26 to 0.8 s. Before,
  kernels retried for about two minutes and userspace clients for 3.7 minutes,
  ending in a false success.
- With cephx, any tampering with the banner is detected.
- The full msgr test suite passes with GREASE on and the extended set checked
  on every connection.
- Sending the set in new frames after authentication caused a reconnect storm.
  That storm is why the set goes in the banner.
- Clients connected before a requirement starts keep working, then hang once
  they need a fresh ticket (see "Turning a requirement on").

.. list-table::
   :header-rows: 1

   * - Experiment
     - What ran
     - Result
   * - msgr unit tests
     - Full ``ceph_test_msgr`` on the prototype, with GREASE on and the
       extended set checked on every connection made by every test, including
       connection races, socket-failure injection and stress
     - 41 of 41 pass; 20 repeated runs of the new tests show no flakiness
   * - Feature-set type
     - 10 new unit tests, 100,000 random inputs to the decoder, a mutation
       check
     - Pass; 10 of 10 mutants caught; no large allocation from hostile lengths
   * - Older peers and the longer banner
     - Released clients and mons from Nautilus 14.2.22 to Umbrella 21.2.0,
       main's mon and OSD, and kernel 6.12, with up to 65,519 extra bytes, in
       both directions
     - All ignore it and work
   * - Kernels
     - Patched 7.3, unpatched 7.3 and 6.12 (secure and crc) over msgr2; 5.4,
       6.12, 7.3 and patched 7.3 over msgr1. CephFS mount and write, RBD map
       and checksum
     - All work when nothing is required. All are refused in 0.26 to 0.37 s
       when something is (124.6 s before, ending in "RADOS feature set
       mismatch"). Patched kernels print the mon's reason; unpatched ones log
       -93.
   * - Userspace refusal
     - Prototype client and upstream main client, over msgr2 and msgr1
     - Refused in 0.27 s (220 s and a false success before)
   * - A running client refused on reconnect
     - ``ceph -w`` running, the mon restarted with a requirement the client
       fails, then the requirement lifted
     - Retries at 1, 10, 15 and 23 s; reconnected as soon as the requirement
       was lifted
   * - Every released client
     - Luminous to Umbrella with nothing required, an extended feature
       required, ``CRUSH_MSR`` required, then the requirement lifted; msgr2 and
       msgr1
     - All work when nothing is required. All are refused in 0.4 to 0.8 s when
       an extended feature is required. With ``CRUSH_MSR``, Octopus to Reef are
       refused in 0.7 s (110.5 s before), and Luminous to Nautilus get in
       through stale bits, as on main today. All are back in at once when the
       requirement goes.
   * - Clients connected when a requirement starts
     - 3 mons, service and mon tickets of 60 and 120 s; a patched client hiding
       the feature, main's and a released Reef client writing; the requirement
       set, every mon restarted, 7 minutes later an OSD restarted, then the
       requirement lifted
     - All three kept writing for 7 minutes, then hung for good at the OSD
       restart (the OSD rejected their expired tickets); the control client
       went on. None came back when the requirement was lifted: main's and Reef
       never retry, and the patched client got ``EACCES`` for a ticket whose
       secret had rotated away. Lifted 40 s after the refusal instead, the
       patched client was back within 30 s; main's never was
   * - Two strikes with several mons
     - Released Reef and Quincy clients lacking ``CRUSH_MSR``, hunting 1, 3 or
       5 mons
     - Refused in 0.6, 1.0 and 1.0 to 1.4 s; strikes are per mon, so a client
       is refused at identification at most once per mon
   * - A stale mon outside the quorum
     - 3 mons; the quorum lifts a requirement while the third, out of quorum,
       keeps it
     - Before mons outside the quorum stopped refusing: 30 of 30 prototype
       clients failed; main's clients, which keep hunting, got in
   * - ``auth none`` over msgr1
     - A requirement the clients lack, cephx off, msgr1 only
     - Before the fix, the refusal named protocol ``none``, whose clients take
       any reply as success: 30 s and ``ETIMEDOUT`` instead of a refusal
   * - An older CLI
     - Main's and Reef's ``ceph`` with a requirement they lack
     - Refused in 0.26 to 0.40 s with "Permission denied"; after ``config set``
       through every mon's admin socket, main's CLI removed the requirement
   * - Malformed banners
     - 17 crafted banners sent to a live mon: truncated, odd and huge lengths,
       65 and 8,000 words, GREASE only, unknown bits, trailing bytes
     - No crash; every malformed one closes the connection at once; the
       well-formed ones are accepted
   * - Crimson
     - A crimson OSD (cyanstore) with this design's mons, mgr and clients,
       GREASE on: I/O, a 20 s bench, a client requirement, a crimson restart
     - Works: about 220,000 writes, the restart rejoins, nothing about banners
       or features in its log; ``ceph features extended`` shows it as unable to
       exchange extended features. The mgr crashed in libcephsqlite on that OSD
       whether extended features were on or off, so that is unrelated
   * - Mon log volume
     - 100 refused connections at the default debug levels
     - 1.1 to 1.6 log lines (290 to 490 bytes) each before per-attempt logging
       moved to level 10
   * - A mon down
     - 3 mons, one killed; clients that lack a required feature
     - Prototype client refused in 0.26 to 0.33 s (30 s without the hunting
       fix); released clients wait out ``client_mount_timeout``
   * - Mixed-version mon quorum
     - Prototype mons with main's mons, as peons and as leader
     - The quorum forms and holds in every mix. Main's mons do not enforce
       requirements, so a requirement must wait for an all-W quorum.
   * - Rolling upgrade
     - Main to the prototype, mons, then OSDs, then the mgr, one at a time,
       under 10 minutes of writes
     - No write errors in two runs (32,777 writes of 4 MiB, then 1.5 million of
       16 KiB); every upgraded daemon reports the extended set, and a prototype
       client writes afterwards
   * - Fault-injection soak
     - 3 mons and 4 OSDs, socket failures injected in daemons and clients, 12
       OSD kill -9 and 3 mon restarts during 8 minutes of writes, GREASE on
     - Both writers finished (7,248 and 7,262 writes); all PGs clean; no
       extended-feature errors; no crashes
   * - Man in the middle
     - A proxy that changes the banner, under cephx crc, cephx secure and auth
       ``none``
     - cephx detects every change (denial of service only); auth ``none``
       allows a silent strip; a required feature stripped means refusal, never
       silent admission
   * - Refusing after authentication, for comparison
     - The running-client case with new frames sent after authentication
     - A reconnect storm of about 300 connections a second, until the host ran
       out of ports
   * - Handshake cost
     - Bytes of a mon connection counted by a proxy, with extended features on
       and off at each end
     - Exactly 20 bytes more each way (a 36-byte banner payload instead of 16);
       no extra frame or round trip
   * - Umbrella port
     - Steps 1 and 2 ported to the Umbrella pool-migration branch (`PR #69112
       <https://github.com/ceph/ceph/pull/69112>`__); pool migration active;
       real clients from Umbrella back to Luminous, kernels 6.12 and 5.4, msgr2
       and msgr1
     - Refused in 0.1 to 1.1 s (55.6 s, 105.3 s and 61 s before); capable
       clients unaffected
   * - Umbrella standalone test
     - ``qa/standalone/mon/pool-migration-clients.sh`` on the Umbrella port
     - Found the hunting bug (a refused client took 60 s); passes with the fix

Problems found in current code along the way:

- three MonClient bugs: the false success (`tracker #82890
  <https://tracker.ceph.com/issues/82890>`__), and, once the mon refuses at
  authentication, no recovery after a failed re-authentication and hunting on
  after a refusal;
- a ``test_msgr`` macro changes ``Policy``'s layout between the test and the
  library, so ``FeatureTest`` has been passing for the wrong reason;
- Luminous to Nautilus clients falsely pass ``HAVE_FEATURE`` for ``CRUSH_MSR``,
  ``SERVER_TENTACLE``, ``SERVER_UMBRELLA`` and ``SERVER_VAMPIRE``, because they
  still send old meanings of reused bits. Confirmed live: they connect to a
  cluster with an MSR rule;
- likely, but not tested: the mon's existing msgr1 authentication-failure path
  closes the connection straight after its reply, so a kernel can miss the
  error.

Not yet done:

- run crimson against the longer banner (a code check shows that it reads the
  whole banner payload and ignores what follows the two words);
- write the health check.
