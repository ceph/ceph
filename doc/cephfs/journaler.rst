===========
 Journaler
===========

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
