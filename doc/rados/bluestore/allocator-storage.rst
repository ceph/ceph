=======================
 Allocator persistence
=======================

.. index:: bluestore; rocksdb; allocator

Pre - Vampire
=============

Beginning with the Pacific release, BlueStore has the option to select where to preserve allocator state.
The choice is between RocksDB and BlueFS file.
The only option that controlled the feature was ``bluestore_allocation_from_file``.

.. confval:: bluestore_allocation_from_file

:doc:`See also <fast-onode-scan>`

The option was aggressive. It was checked during BlueStore initialization.
If turned on, BlueStore would ditched RocksDB stored allocator and switched to file stored allocator.
The actual condition to perform the transition was more complicated than just reading ``bluestore_allocation_from_file``.
The fear of long recovery times was a reason for the decision to stick to RocksDB allocator storage for rotational devices.

DB / File allocator storage selection
-------------------------------------

+---------------+-----------------+------------------+
|               | MAIN=rotational | MAIN=solid       |
+===============+=================+==================+
| DB=none       | RocksDB         | Config           |
+---------------+-----------------+------------------+
| DB=rotational | RocksDB         | RocksDB          |
+---------------+-----------------+------------------+
| DB=solid      | Config          | Config           |
+---------------+-----------------+------------------+

``Config`` = value determined by ``bluestore_allocation_from_file``.


Vampire
=======

Vampire exposes and simplifies the logic around allocator storage.

.. confval:: bluestore_allocation_from_file_rotational

The previous mandatory RocksDB allocator storage on rotational devices has been turned into an option.
Where pre-Vampire rotational device forced allocations to stay on RocksDB,
the option ``bluestore_allocation_from_file_rotational`` gives the choice instead.

DB / File allocator storage selection
--------------------------------------

+---------------+-----------------+----------------+
|               | MAIN=rotational | MAIN=solid     |
+===============+=================+================+
| DB=none       | Alt.Config      | Config         |
+---------------+-----------------+----------------+
| DB=rotational | Alt.Config      | Alt.Config     |
+---------------+-----------------+----------------+
| DB=solid      | Config          | Config         |
+---------------+-----------------+----------------+

``Config`` = value from ``bluestore_allocation_from_file``.
``Alt.Config`` = value from ``bluestore_allocation_from_file_rotational``.

Conversion trigger
-----------------------------------------

Vampire discontinued the logic of automatic mandatory conversion from DB to file allocator storage.
Instead new variable controls when to run the conversion.

.. confval:: bluestore_allocation_from_file_convert_on_mount

The allocation target to convert to is selected in the usual way.


