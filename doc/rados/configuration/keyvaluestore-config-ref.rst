============================
 KeyValueStore Config Reference
============================

KeyValueStore is a another OSD backend compared to FileStore. Now it mainly
use LevelDB as backend.

KeyValueStore doesn't need jounal device, each op will flush into backend
directly.

``keyvaluestore backend``

:Description: the backend used by keyvaluestore
:Type: String
:Required: No
:Default: ``leveldb``


.. index:: keyvaluestore; queue

Queue
=====

The following settings provide limits on the size of keyvaluestore queue.

``keyvaluestore queue max ops``

:Description: Defines the maximum number of in progress operations the keyvaluestore accepts before blocking on queuing new operations.
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

:Description: The number of keyvaluestore operation threads that execute in parallel. 
:Type: Integer
:Required: No
:Default: ``2``


``keyvaluestore op thread timeout``

:Description: The timeout for a keyvaluestore operation thread (in seconds).
:Type: Integer
:Required: No
:Default: ``60``


``keyvaluestore op thread suicide timeout``

:Description: The timeout for a commit operation before cancelling the commit (in seconds). 
:Type: Integer
:Required: No
:Default: ``180``


Misc
====


``keyvaluestore default strip size``

:Description: each object will be split into multi key/value pairs stored into
              backend.
              Note: now this option is important to performance for estimable
              workload
:Type: Integer
:Required: No
:Default: ``4096``


``keyvaluestore header cache size``

:Description: the number of header cache, it just like "inode" in local
              filesystem. The larger cache size will be help for performance

:Type: Integer
:Required: No
:Default: ``4096``
