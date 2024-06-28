==============================
The ``ClientRequest`` pipeline
==============================

In crimson, exactly like in the classical OSD, a client request has data and
ordering dependencies which must be satisfied before processing (actually
a particular phase of) can begin. As one of the goals behind crimson is to
preserve the compatibility with the existing OSD incarnation, the same semantic
must be assured. An obvious example of such data dependency is the fact that
an OSD needs to have a version of OSDMap that matches the one used by the client
(``Message::get_min_epoch()``).

If a dependency is not satisfied, the processing stops. It is crucial to note
the same must happen to all other requests that are sequenced-after (due to
their ordering requirements).

There are a few cases when the blocking of a client request can happen.


  ``ClientRequest::ConnectionPipeline::await_map``
    wait for particular OSDMap version is available at the OSD level
  ``ClientRequest::ConnectionPipeline::get_pg``
    wait a particular PG becomes available on OSD
  ``ClientRequest::PGPipeline::await_map``
    wait on a PG being advanced to particular epoch
  ``ClientRequest::PGPipeline::wait_for_active``
    wait for a PG to become *active* (i.e. have ``is_active()`` asserted)
  ``ClientRequest::PGPipeline::recover_missing``
    wait on an object to be recovered (i.e. leaving the ``missing`` set)
  ``ClientRequest::PGPipeline::get_obc``
    wait on an object to be available for locking. The ``obc`` will be locked
    before this operation is allowed to continue
  ``ClientRequest::PGPipeline::process``
    wait if any other ``MOSDOp`` message is handled against this PG

At any moment, a ``ClientRequest`` being served should be in one and only one
of the phases described above. Similarly, an object denoting particular phase
can host not more than a single ``ClientRequest`` the same time. At low-level
this is achieved with a combination of a barrier and an exclusive lock.
They implement the semantic of a semaphore with a single slot for these exclusive
phases.

As the execution advances, request enters next phase and leaves the current one
freeing it for another ``ClientRequest`` instance. All these phases form a pipeline
which assures the order is preserved.

These pipeline phases are divided into two ordering domains: ``ConnectionPipeline``
and ``PGPipeline``. The former ensures order across a client connection while
the latter does that across a PG. That is, requests originating from the same
connection are executed in the same order as they were sent by the client.
The same applies to the PG domain: when requests from multiple connections reach
a PG, they are executed in the same order as they entered a first blocking phase
of the ``PGPipeline``.

Comparison with the classical OSD
----------------------------------
As the audience of this document are Ceph Developers, it seems reasonable to
match the phases of crimson's ``ClientRequest`` pipeline with the blocking
stages in the classical OSD. The names in the right column are names of
containers (lists and maps) used to implement these stages. They are also
already documented in the ``PG.h`` header.

+----------------------------------------+--------------------------------------+
| crimson                                | ceph-osd waiting list		|
+========================================+======================================+
|``ConnectionPipeline::await_map``       | ``OSDShardPGSlot::waiting`` and	|
|``ConnectionPipeline::get_pg``          | ``OSDShardPGSlot::waiting_peering``	|
+----------------------------------------+--------------------------------------+
|``PGPipeline::await_map``               | ``PG::waiting_for_map``		|
+----------------------------------------+--------------------------------------+
|``PGPipeline::wait_for_active``         | ``PG::waiting_for_peered``		|
|                                        +--------------------------------------+
|                                        | ``PG::waiting_for_flush``		|
|                                        +--------------------------------------+
|                                        | ``PG::waiting_for_active``		|
+----------------------------------------+--------------------------------------+
|To be done (``PG_STATE_LAGGY``)         | ``PG::waiting_for_readable``		|
+----------------------------------------+--------------------------------------+
|To be done                              | ``PG::waiting_for_scrub``		|
+----------------------------------------+--------------------------------------+
|``PGPipeline::recover_missing``         | ``PG::waiting_for_unreadable_object``|
|                                        +--------------------------------------+
|                                        | ``PG::waiting_for_degraded_object``	|
+----------------------------------------+--------------------------------------+
|To be done (proxying)                   | ``PG::waiting_for_blocked_object``	|
+----------------------------------------+--------------------------------------+
|``PGPipeline::get_obc``                 | *obc rwlocks*			|
+----------------------------------------+--------------------------------------+
|``PGPipeline::process``                 | ``PG::lock`` (roughly)		|
+----------------------------------------+--------------------------------------+


As the last word it might be worth to emphasize that the ordering implementations
in both classical OSD and in crimson are stricter than a theoretical minimum one
required by the RADOS protocol. For instance, we could parallelize read operations
targeting the same object at the price of extra complexity but we don't -- the
simplicity has won.
