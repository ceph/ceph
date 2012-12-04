===========
 Messaging
===========


``ms tcp nodelay``

:Description: Disables nagle's algorithm on messenger tcp sessions.
:Type: Boolean
:Required: No
:Default: ``true``


``ms initial backoff``

:Description: 
:Type: Double
:Required: No
:Default: ``.2``


``ms max backoff``

:Description: 
:Type: Double
:Required: No
:Default: ``15.0``


``ms nocrc``

:Description: Disables crc on network messages.  May increase performance if cpu limited.
:Type: Boolean
:Required: No
:Default: ``false``


``ms die on bad msg``

:Description: 
:Type: Boolean
:Required: No
:Default: ``false``


``ms dispatch throttle bytes``

:Description: Throttles total size of messages waiting to be dispatched.
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``100 << 20``


``ms bind ipv6``

:Description: 
:Type: Boolean
:Required: No
:Default: ``false``


``ms rwthread stack bytes``

:Description: 
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``1024 << 10``


``ms tcp read timeout``

:Description: 
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``900``


``ms inject socket failures``

:Description: 
:Type: 64-bit Unsigned Integer
:Required: No
:Default: ``0``
