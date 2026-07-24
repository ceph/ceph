# src/mds/ — Metadata Server

MDS manages the CephFS namespace: in-memory cache of inodes (`CInode`), dentries (`CDentry`), directory fragments (`CDir`), distributed locking, and journaling.

`MDCache.cc` is ~15K lines — search by function rather than reading linearly.

`frag_t` (directory fragmentation) uses a **bitwise prefix scheme**. Understanding it is required for any work on directory operations.

`Locker.cc` is ~6K lines of cap/lock state transitions. The locking model is intricate and easy to break.

CInode/CDentry/CDir use intrusive reference counting — don't hold raw pointers across async operations.
