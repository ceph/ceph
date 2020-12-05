=========
Pipelines
=========

RADOS Protocol Requirements
===========================

Requests
--------

OSD serves different kinds of requests. Some of them are targeting the local state of the connected
OSD instance. For instance, the ``ceph config get`` command is directed straight to the local config
subsystem, and is fulfilled right away. But others inquiry or mutate the shared state of the cluster.
Following are two categories of these requests:

- I/O requests from clients.
- PG peering requests from peer OSDs.

Because inherently a distributed storage system is distributed by its own nature, we can not assume
that an OSD or the PGs hosted by it are always ready to serve a request targeting a shared state, not
to mention the unpredictable hardware failures. But fortunately, even if an OSD is not able to serve
a request right away, under most circumstances, it can postpone the request, prepare itself so that it
can answer this request. It's quite common in daily life:

  Great question, let me look into it and I will get back to you!

Some questions involves multiple steps. To serve an I/O request from client, at
least 7 stages are involved:

.. ditaa::

   +----+---------------------+
   |cGRE| OSD wait for osdmap |
   +----+----+----------------+
             |
             V
   +----+-----------------+
   |cGRE| OSD wait for PG |
   +----+-----------------+
             |
             V
   +----+--------------------+
   |cBLU| PG wait for osdmap |
   +----+--------------------+
             |
             V
   +----+--------------------+
   |cBLU| PG wait for active |
   +----+--------------------+
             |
             V
   +----+----------------------+
   |cBLU| PG wait for recovery |
   +----+----------------------+
             |
             V
   +----+---------------------+
   |cBLU| wait for object ctx |
   +----+---------------------+
             |
             V
   +----+------------------+
   |cBLU| wait for process |
   +----+------------------+

             legend
             +----------------+
             |cGRE OSD stages |
             +----------------+
             |cBLU PG stage   |
             +----------------+

Two Requirements
----------------

An OSD client talks with each connected OSD over stateful connection. The client is allowed to
perform asynchronous I/O with OSD in the sense that it can send another request without waiting
for the completion of the previous one. But the contract of the RADOS protocol requires writes
(and write ordered reads) performed on a single object to be performed in the order in which
they were originally submitted, and only once. This behavior is implemented by both the client
side and server side of librados.

So far, we have two restrictions:

#. sequential stages of a given request
#. ordered execution of multiple write requests on a single object

It's like visiting a history museum with friends.

#. since the exhibition rooms in the museum are arranged in chronological order, it is advisable
   to visit these rooms in the same order.
#. before leaving the museum, visitors can leave a message on one of the notebooks provided by
   the museum. But the visitors should do this in the same order in which they enter the museum.
   For example, if both Joe and Jane decide to leave a message on the notebook with purple cover,
   and Joe enters the museum before Jane, Jane cannot write on the notebook until Joe does.

Current Implementation
======================

We, as the staff of the museum, are responsible to codify these guidelines. It is straightforward
to follow the first rule. We can setup a pipeline for the exhibition rooms so each room will be
a stage in the pipeline. Once a visitor finishes visiting a room, he or she will be transferred to
the next room until the last one. But regarding the second rule, we have a couple solutions:

- prohibit more than 1 person staying in a single room. The notebooks are located in the last room.
  This way, the order in which visitors leave messages would be exactly the same as the order they
  enter the museum.
- allow more than 1 person staying in a single room as long as the number of the visitor does not
  exceeds the capacity of the room. The staff of museum should give the purple handbook to the first
  visitor who registers to write on this handbook, so the visitor can hand it over to the next one
  or simply return it to the staff once she leaves her message on it. Visitors can pick a notebook
  before entering the museum, and they should follow this protocol.

Apparently, the first solution is way simpler. It **satisfies** both of the requirements, but its
downside is that a slow visitor could keep others out of the door for long time, and hence increases
the overall waiting time. This could be worse if we have multiple visitors who keep the same visitor(s)
waiting for different rooms.

Crimson uses the first solution at the time of writing.

.. ditaa::

   +----+--------+                                             |
   |cGRE| osdmap +-+                                           |
   +----+--------+ |                                           | time
                   |                                           |
                   V                                           |
                 +----+----+                                   V
                 |cGRE| PG +-+
                 +----+----+ |
                             |
   +----+--------+           V
   |cGRE| osdmap +-+       +----+--------+
   +----+--------+ |       |cBLU| osdmap +-+
                   |       +----+--------+ |
                   V                       |
                 +----+----+               V
                 |cGRE| PG +-+           +----+--------+
                 +----+----+ |           |cBLU| active +-+
                             |           +----+--------+ |
                             V                           |
                           +----+--------+               V
                           |cBLU| osdmap +-+           +----+----------+
                           +----+--------+ |           |cBLU| recovery +-+
                                           |           +----+----------+ |
                                           V                             |
                                         +----+--------+                 V
                                         |cBLU| active +-+             +----+---------+
                                         +----+--------+ |             |cBLU| obj ctx +-+
                                                         |             +----+---------+ |
                                                         V                              |
                                                       +----+----------+                V
                                                       |cBLU| recovery +-+             +----+---------+
                                                       +----+----------+ |             |cBLU| process |
                                                                         |             +----+---------+
                                                                         V
                                                                       +----+---------+
                                                                       |cBLU| obj ctx +-+
                                                                       +----+---------+ |
                                                                                        |
                                                                                        V
                                                                                       +----+---------+
                                                                                       |cBLU| process |
                                                                                       +----+---------+

In the diagram above, two requests from the same client are being served in parallel. They share the same
pipeline. But before one transits to the next stage, it have to wait until the previous one leaves that stage.
And please note, the last "process" stage is a per-PG stage. So only one request is allowed to stay in that
stage at any give point of time.

Future Improvement
==================

At first glance, the product of the number of PGs and the number of stages in the pipeline determines the level
of parallelism. Some of us might argue, can we improve the throughput using some well known techniques? But
please also bear in mind that the performance is always evaluated under a particular workload. So we need to
analysis the possible improvements on a case-by-case basis.

Reordering
----------

When serving requests from a client performing asynchronous I/O, OSD has the opportunity to execute
multiple requests from the same client in parallel. But due to the second restriction, we should always
reply the client in-order. But this does not prevent us from **executing** the requests ouf-of-order, as
long as the reordering does not change the behavior of the execution or breaks the dependencies between
requests.

- for read requests, the "behavior" is the returned metadata and payload of object.
- for write requests, the "behavior" is the state transition of the object storage of this PG shard and its peers.

Let's consider a use case of read-read reordering, where the client sends two read requests targeting the
same object in a row. Transposing these two read operation does not change the corresponding replies, unless
there is a write request sneaked in after the first read request is served. But since the client sending
read requests is not the one sending write request, the former should not be annoyed if the reordering
changes the responses -- the object state is still consistent, and the contract of RADOS protocol is not
broken.

When it comes to read-write reordering, we cannot ensure the consistency of the behavior without checking
if the extents of read and write operations overlap with each other. The same applies to write-write reordering.

Please note, we don't ensure the ordering of execution from different clients. The RADOS application should use
an application-level protocol to ensure this. Also, we are perfectly allowed to reorder a read ahead of a write
on a particular object as long as the read isn't write ordered -- we will do so, for instance, if the object is
degraded, but readable.

But can we really observe a performance improvement under real-world workloads if OSD is able to reorder
client requests?

Let's take a look at a use case: a certain client sends 10 consecutive requests targeting different objects.

Since these requests are targeting different objects, The status of the objects would be eventually consistent
no matter in which order the write requests are executed, even if these requests are writes. It sounds like a
nice scatter/gather I/O at a higher level. But this does not really happen in real world. Because, most librados
applications, RBD for instance, do not work in terms of a single huge RADOS object. They slice a large object
into smaller fixed-size chunks based on offset, and store these chunks as bounded size RADOS objects. An RBD
image is always mapped to multiple RADOS objects, which are in turn placed in different PGs hosted by differnt
OSDs. So, 10 requests on an RBD device won't be pipelined in general. It's more unlikely than getting 10 heads
in row when tossing coins.


More than 1 Visitor in a Room
-----------------------------

Some stages in the pipeline above do not really "process" requests. They are more like gatekeepers.
For example, if a client request is waiting for the OSD to be updated with an updated osdmap, its
next request will also very likely to be waiting for the same map if it is allowed to enter that stage.
So, in this case, allowing multiple request from the same client to wait for the same gatekeepers in
the same stage does not help.

But the last stage in the pipeline does process requests. The "process" stage calls into the PG
backend for performing I/O. It even recovers the object if the object to be accessed is not available
or corrupted. "Process" stage is a per-PG pharse. Namely, each PG has only a single "room" for
processing requests. So, a PG is not able to handle multiple operations at a time due to this design.

- for multiple write requests targeting the same object.

  It really doesn't make sense to execute multiple writes on the same object at once. Because we already
  do the only sensible version by chopping up an RBD image into multiple 4MB chunks. We are not likely
  to benefit from doing so.
- for multiple write requests targeting different object. These requests are sent by different clients.

  We will have to ensure the ACID of the object store with a more sophisticated design.
- for multiple read requests targeting the same object.

  This could be a typical use case where multiple services are setup and teardown very frequently. Image
  that we have a cluster backing tens of thousands of services. All of them are based on a certain
  container image. So, those RADOS objects containing the container image would be **very** popular. If
  a PG is able to serve multiple read requests concurently, these requests are likely to be replied sooner.
  We will need to drain existing writers before start serving readers, and vice versa.
- for multiple read requests targeting different objects.

  If we thought that we'd actually get enough reads on the same pg in parallel to justify the complexity,
  we actually could permit overlapping reads within a PG or a single object.

Let's take a step back. The parallelism is not only determined by the room's capacity, it can also be
contributed by the number of museums! Normally, each OSD hosts hundreds of PGs. The chance that the
throughput is limited by the bottleneck in PG "process" stage is relatively low, if the I/O request
is distrbuted evenly across the PGs.
