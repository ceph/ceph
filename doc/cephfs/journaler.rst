===========
 Journaler
===========

``journaler allow split entries``

:Description: Allow an entry to span a stripe boundary
:Type: Boolean
:Required: No
:Default: ``true``


``journaler write head interval``

:Description: How frequently to update the journal head object
:Type: Integer
:Required: No
:Default: ``15``


``journaler prefetch periods``

:Description: How many stripe periods to read-ahead on journal replay
:Type: Integer
:Required: No
:Default: ``10``


``journal prezero periods``

:Description: How mnay stripe periods to zero ahead of write position
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
