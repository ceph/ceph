====================
OP Lock Completion
====================

Motivation
----------
Current write operation needs acquiring and releasing the PG lock frequently.
It causes a huge overhead in the case of heavy random write workload.
Therefore, this patch focus on minimizing the PG lock contention.

Concepts
--------
The main Concept is removing the PG lock when OSD handles completion processing
such as committed, applied, receiving reply from replica. OSD will send ack to
client with simple counting under the OP lock. Updating metadatas which
need the PG are delayed. Delayed works will be re-enqueue to ShardedOpWQ threads or
it will be handled existing write operation.


*Write Flow*
A. when a write request is issued (Primary)
1. PG lock (acquire) -> 2. do_completion() -> 3. repop queue lock ->
4. add RepGather to repop_queue -> 5. add InProgressOP (ReplicateBackend) or
Op (ECBackend) -> 6. repop queue lock (release) -> 7. queue_transaction() ->
8. PG lock (release)

B. when a write request is issued (Not primary)
1. PG lock (acquire) -> 2. do_completion() -> 3. add RepSub to
repop_sub_queue -> 4. add RepModify (ReplicateBackend) or Op (ECBackend) ->
5. queue_transaction() -> 6. PG lock (release)

C. when commit, apply are completed
1. complete commit or apply. -> 2. Repop(OP) lock (acquire) ->
3. If all commit or apply are finished, send ack to client ->
4. Repop(OP) lock (release) -> 5. _queue_for_completion()

D. when osd receives MSG_OSD_REPOPREPLY or MSG_OSD_EC_WRITE_REPLY
1. repop queue lock -> 2. find out OP -> 3. Repop(OP) lock (acquire) ->
4. check completion (applied or commit) -> 5. If all commit or
apply are finished, send ack to client -> 6. Repop(OP) lock (release) ->
7. _queue_for_completion() -> 8. repop queue lock (release)

E. _queue_for_completion (using ShardedOpWQ threads)
1. PG lock (acquire) -> 2. do_completion() (iterate repop_queue and
repop_sub_queue for completion and remove RepGather and RepSub from
repop_queue and repop_sub_queue) -> 3. PG lock (release)


*Data Structure*

A. CompletionItem
See src/osd/PG.h

The CompletionItem abstracts the RepGather and the RepSub for op lock completion.
The CompletionItem contain the op lock and is used for op_comp_context(),
do_completion().

*Implication*

A. Recovery cases
Most of Recovery cases such as push, pull can be processed without side effect.
Because this patch does not modify RW ordering (Operations for Recovery can be
blocked until RW state is freed). Only one thing we should be considered is
on_change(). on_change() will be called when the PG state is changed. In that
case, We should cancle existing operations as existing codes.

B. Scrub case
Existing scrub has no side effect from this patch. Because Srub will be wait
until last_update (in pg_log) operation is finished.

C. Ordering
ShardedOpWQ(submit write)
the PG lock will be used for submitting OP as same as existing code.

Commit, Apply finishers
The OP lock will be used. However each finisher handles completed operations
in order. Because single finisher which in charge of sepecific PG processes
requests via queueing sequence.

Processing reply (commit or apply ack)
repop_queue_lock can be protect ordering. the PG lock is not needed.

Completion
In the case of processing completion, do_completion will be check the front
item in repop_queue. Therefore completion is handled as submitted order.

