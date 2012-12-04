===========
 Messaging
===========


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
