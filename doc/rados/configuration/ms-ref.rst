===========
 Messaging
===========

General Settings
================

``ms_tcp_nodelay``

:Description: Disables Nagle's algorithm on messenger TCP sessions.
:Type: Boolean
:Required: No
:Default: ``true``


``ms_initial_backoff``

:Description: The initial time to wait before reconnecting on a fault.
:Type: Double
:Required: No
:Default: ``.2``


``ms_max_backoff``

:Description: The maximum time to wait before reconnecting on a fault.
:Type: Double
:Required: No
:Default: ``15.0``


``ms_nocrc``

:Description: Disables CRC on network messages.  May increase performance if CPU limited.
:Type: Boolean
:Required: No
:Default: ``false``


``ms_die_on_bad_msg``

:Description: Debug option; do not configure.
:Type: Boolean
:Required: No
:Default: ``false``


``ms_dispatch_throttle_bytes``

:Description: Throttles total size of messages waiting to be dispatched.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``100 << 20``


``ms_bind_ipv6``

:Description: Enable to bind daemons to IPv6 addresses instead of IPv4. Not required if you specify a daemon or cluster IP.
:Type: Boolean
:Required: No
:Default: ``false``


``ms_rwthread_stack_bytes``

:Description: Debug option for stack size; do not configure.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``1024 << 10``


``ms_tcp_read_timeout``

:Description: Controls how long (in seconds) the messenger will wait before closing an idle connection.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``900``


``ms_inject_socket_failures``

:Description: Debug option; do not configure.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``0``

Async messenger options
=======================


``ms_async_transport_type``

:Description: Transport type used by Async Messenger. Can be ``posix``, ``dpdk``
              or ``rdma``. Posix uses standard TCP/IP networking and is default. 
              Other transports may be experimental and support may be limited.
:Type: String
:Required: No
:Default: ``posix``


``ms_async_op_threads``

:Description: Initial number of worker threads used by each Async Messenger instance.
              Should be at least equal to highest number of replicas, but you can
              decrease it if you are low on CPU core count and/or you host a lot of
              OSDs on single server.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``3``


``ms_async_send_inline``

:Description: Send messages directly from the thread that generated them instead of
              queuing and sending from Async Messenger thread. This option is known
              to decrease performance on systems with a lot of CPU cores, so it's
              disabled by default.
:Type: Boolean
:Required: No
:Default: ``false``
