=========
 Journal 
=========

Journal
=======

``journal dio``

:Description: 
:Type: Boolean
:Required: No
:Default: ``true``


``journal aio``

:Description: 
:Type: Boolean 
:Required: No
:Default: ``false``


``journal block align``

:Description: 
:Type: Boolean
:Required: No
:Default: ``true``


``journal max write bytes``

:Description: 
:Type: Integer
:Required: No
:Default: ``10 << 20``


``journal max write entries``

:Description: 
:Type: Integer
:Required: No
:Default: ``100``


``journal queue max ops``

:Description: 
:Type: Integer
:Required: No
:Default: ``500``


``journal queue max bytes``

:Description: 
:Type: Integer
:Required: No
:Default: ``10 << 20``


``journal align min size``

:Description: Align data payloads greater than the specified minimum.
:Type: Integer
:Required: No
:Default: ``64 << 10``


``journal replay from``

:Description: 
:Type: Integer
:Required: No
:Default: ``0``


``journal zero on create``

:Description: 
:Type: Boolean
:Required: No
:Default: ``false``


Journaler
=========

``journaler allow split entries``

:Description: 
:Type: Boolean
:Required: No
:Default: ``true``


``journaler write ahead interval``

:Description: 
:Type: Integer
:Required: No
:Default: ``15``


``journaler prefetch periods``

:Description: 
:Type: Integer
:Required: No
:Default: ``10``


``journal prezero periods``

:Description: 
:Type: Integer
:Required: No
:Default: ``10``

``journaler batch interval``

:Description: Maximum additional latency in seconds we incur artificially. 
:Type: Double
:Required: No
:Default: ``.001``


``journaler batch max``

:Description: Maximum bytes we'll delay flushing. 
:Type: 64-bit Unsigned Integer 
:Required: No
:Default: ``0``
