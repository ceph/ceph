==============================
The ``ClientRequest`` pipeline
==============================

RADOS requires writes on each object to be ordered.  If a client
submits a sequence of concurrent writes (doesn't want for the prior to
complete before submitting the next), that client may rely on the
writes being completed in the order in which they are submitted.

As a result, the client->osd communication and queueing mechanisms on
both sides must take care to ensure that writes on a (connection,
object) pair remain ordered for the entire process.

crimson-osd enforces this ordering via Pipelines and Stages
(crimson/osd/osd_operation.h).  Upon arrival at the OSD, messages
enter the ConnectionPipeline::AwaitActive stage and proceed
through a sequence of pipeline stages:

* ConnectionPipeline: per-connection stages representing the message handling
  path prior to being handed off to the target PG
* PerShardPipeline: intermediate Pipeline representing the hand off from the
  receiving shard to the shard with the target PG.
* CommonPGPipeline: represents processing on the target PG prior to obtaining
  the ObjectContext for the target of the operation.
* CommonOBCPipeline: represents the actual processing of the IO on the target
  object

Because CommonOBCPipeline is per-object rather than per-connection or
per-pg, multiple requests on different objects may be in the same
CommonOBCPipeline stage concurrently.  This allows us to serve
multiple reads in the same PG concurrently.  We can also process
writes on multiple objects concurrently up to the point at which the
write is actually submitted.

See crimson/osd/osd_operations/client_request.(h|cc) for details.
