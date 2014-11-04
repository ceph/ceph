===============================
 KeyValueStore Config Reference
===============================

``KeyValueStore`` is an alternative OSD backend compared to FileStore.
Currently, it uses LevelDB as backend. ``KeyValueStore`` doesn't need journal
device. Each operation will flush into the backend directly.


``keyvaluestore backend``

:Description: The backend used by ``KeyValueStore``.
:Type: String
:Required: No
:Default: ``leveldb``


.. index:: keyvaluestore; queue

Queue
=====

The following settings provide limits on the size of the ``KeyValueStore`` 
queue.

``keyvaluestore queue max ops``

:Description: Defines the maximum number of  operations in progress the 
              ``KeyValueStore`` accepts before blocking on queuing new operations.

:Type: Integer
:Required: No. Minimal impact on performance.
:Default: ``50``


``keyvaluestore queue max bytes``

:Description: The maximum number of bytes for an operation.
:Type: Integer
:Required: No
:Default: ``100 << 20``

.. index:: keyvaluestore; thread

Thread
========


``keyvaluestore op threads``

:Description: The number of ``KeyValueStore`` operation threads that execute in parallel. 
:Type: Integer
:Required: No
:Default: ``2``


``keyvaluestore op thread timeout``

:Description: The timeout for a ``KeyValueStore`` operation thread (in seconds).
:Type: Integer
:Required: No
:Default: ``60``


``keyvaluestore op thread suicide timeout``

:Description: The timeout for a commit operation before canceling the commit (in seconds). 
:Type: Integer
:Required: No
:Default: ``180``


Misc
====


``keyvaluestore default strip size``

:Description: Each object will be split into multiple key/value pairs and 
              stored in the backend. **Note:** The size of the workload has 
              a significant impact on performance.
:Type: Integer
:Required: No
:Default: ``4096``


``keyvaluestore header cache size``

:Description: The size of the header cache (identical to ``inode`` in the local
              filesystem). A larger cache size enhances performance.

:Type: Integer
:Required: No
:Default: ``4096``
