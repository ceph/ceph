===========
 Messaging
===========

General Settings
================

``ms tcp nodelay``

:Description: Disables nagle's algorithm on messenger tcp sessions.
:Type: Boolean
:Required: No
:Default: ``true``


``ms initial backoff``

:Description: The initial time to wait before reconnecting on a fault.
:Type: Double
:Required: No
:Default: ``.2``


``ms max backoff``

:Description: The maximum time to wait before reconnecting on a fault.
:Type: Double
:Required: No
:Default: ``15.0``


``ms nocrc``

:Description: Disables crc on network messages.  May increase performance if cpu limited.
:Type: Boolean
:Required: No
:Default: ``false``


``ms die on bad msg``

:Description: Debug option; do not configure.
:Type: Boolean
:Required: No
:Default: ``false``


``ms dispatch throttle bytes``

:Description: Throttles total size of messages waiting to be dispatched.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``100 << 20``


``ms bind ipv6``

:Description: Enable if you want your daemons to bind to IPv6 address instead of IPv4 ones. (Not required if you specify a daemon or cluster IP.)
:Type: Boolean
:Required: No
:Default: ``false``


``ms rwthread stack bytes``

:Description: Debug option for stack size; do not configure.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``1024 << 10``


``ms tcp read timeout``

:Description: Controls how long (in seconds) the messenger will wait before closing an idle connection.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``900``


``ms inject socket failures``

:Description: Debug option; do not configure.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``0``

Async messenger options
=======================


``ms async transport type``

:Description: Transport type used by Async Messenger. Can be ``posix``, ``dpdk``
              or ``rdma``. Posix uses standard TCP/IP networking and is default. 
              Other transports may be experimental and support may be limited.
:Type: String
:Required: No
:Default: ``posix``


``ms async op threads``

:Description: Initial number of worker threads used by each Async Messenger instance.
              Should be at least equal to highest number of replicas, but you can
              decrease it if you are low on CPU core count and/or you host a lot of
              OSDs on single server.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``3``


``ms async max op threads``

:Description: Maximum number of worker threads used by each Async Messenger instance. 
              Set to lower values when your machine has limited CPU count, and increase 
              when your CPUs are underutilized (i. e. one or more of CPUs are
              constantly on 100% load during I/O operations).
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``5``


``ms async set affinity``

:Description: Set to true to bind Async Messenger workers to particular CPU cores. 
:Type: Boolean
:Required: No
:Default: ``true``


``ms async affinity cores``

:Description: When ``ms async set affinity`` is true, this string specifies how Async
              Messenger workers are bound to CPU cores. For example, "0,2" will bind
              workers #1 and #2 to CPU cores #0 and #2, respectively.
              NOTE: when manually setting affinity, make sure to not assign workers to
              processors that are virtual CPUs created as an effect of Hyperthreading
              or similar technology, because they are slower than regular CPU cores.
:Type: String
:Required: No
:Default: ``(empty)``


``ms async send inline``

:Description: Send messages directly from the thread that generated them instead of
              queuing and sending from Async Messenger thread. This option is known
              to decrease performance on systems with a lot of CPU cores, so it's
              disabled by default.
:Type: Boolean
:Required: No
:Default: ``false``


