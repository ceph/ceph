==============================
Multi-Object Write Transaction
==============================

Multi-object write transaction enables a group of write operations
on multi-objects complete atomically.

Description of the Process
--------------------------

The high level process is,

  1. Define the following class,

     class *CEPH_RADOS_API MultiObjectWriteOperation*

     {

       std::map<std::string, ObjectWriteOperation*> slaves;

       ObjectWriteOperation master;

     };

     based on this, client could send a group of ops on multi-objects
     through *MOSDOp* to the primary osd in charge of the *master* object
     which is explicitly specified by client, call this osd *MASTER*

  #. *MASTER* recieves the multi-object write operation request, writes a
     *LOCK* entry into *PGLOG*, creates a meta object, and records the *slaves*
     information in it;

  #. *MASTER* extracts operations by objects from *slaves*, enclose them in
     *LOCK* requests, each request includes a group of operations on a single
     slave object. *MASTER* sends *LOCK* requests to corresponding primary osds
     individually, call those osds "SLAVE*;

  #. *SLAVE* recieves *LOCK* request, confirms the validity of the operation,
     then writes a *LOCK* entry into *PGLOG*, creates a meta object, and
     records the transaction information in it, then sends ack to *MASTER*;

  #. *MASTER* collects response, then does the operations on the *master*
     object, besides, writes a *COMMIT* entry into *PGLOG*. *MASTER* sends
     *COMMIT* request to *SLAVE*, then sends ack to client;

  #. *SLAVE* receives *COMMIT*, performs the operations on the slave object,
     besides, writes a *COMMIT* entry into *PGLOG*. *SLAVE* sends ack to
     *MASTER*, deletes the meta object and writes a *UNLOCK* entry into
     *PGLOG*;

  #. *MASTER* collects ack, deletes the meta object and writes a *UNLOCK*
     entry into *PGLOG*

Fault Tolerance
---------------
When restarting, the osd re-creates the transaction in memory according to
the *PGLOG* entry and meta object

- *MASTER* restart

 - If the transaction is in *LOCK* state

 (1) *MASTER* sends *UNLOCK* to *SLAVE*;

 (2) *SLAVE* receives *UNLOCK*, if the transaction does not exist or in
 *UNLOCK* state, goto 3; If the transaction is in *LOCK* state, *SLAVE*
 rolls back;

 (3) *SLAVE* sends ack to *MASTER*;

 (4) *MASTER* collects ack, and rolls back itself.

 - If the transaction is in *COMMIT* state

 (1) *MASTER* sends *COMMIT* to *SLAVE*;

 (2) *SLAVE* receives *COMMIT*, if the transaction is not in *LOCK* state,
 goto 3; Otherwise, proceed as normal;

 (3) *SLAVE* sends ack to *MASTER*;


 (4) *MASTER* collects ack, proceed as normal

- SLAVE restart

 - If the transaction is in *LOCK* state

 *Slave* waits *MASTER* to re/send *LOCK/UNLOCK/COMMIT*

 - If the transaction is in *COMMIT* state

 *SLAVE* does the *UNLOCK* process; if *MASTER* resends *COMMIT*, it directy
 sends ack to *MASTER*

Dead Lock Avoidance
-------------------

When a osd receives a new multi-object write operation request, if there is
pending multi-object write operation operating on the same object,
return *EDEADLK*, except these two cases:

  1. The osd plays *MASTER* for the new operation;

  2. The osd plays *SLAVE* for the new operation, and the pending operation
  has entered *COMMIT* step;

In these two cases, the new request could be delayed until the pending one
finishes

When a new single object write operation request comes, if there is pending
multi-object write operation operating on the same object, the new request must
be delayed until the pending one finish
