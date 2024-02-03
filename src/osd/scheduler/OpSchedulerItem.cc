// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "osd/scheduler/OpSchedulerItem.h"
#include "osd/OSD.h"
#include "osd/osd_tracer.h"


namespace ceph::osd::scheduler {

std::ostream& operator<<(std::ostream& out, const op_scheduler_class& class_id) {
  out << static_cast<size_t>(class_id);
  return out;
}

void PGOpItem::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_op(pg, op, handle);
  pg->unlock();
}

void PGPeeringItem::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_peering_evt(sdata, pg.get(), evt, handle);
}

void PGSnapTrim::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  pg->snap_trimmer(epoch_queued);
  pg->unlock();
}

void PGScrub::run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle)
{
  pg->scrub(epoch_queued, handle);
  pg->unlock();
}

void PGScrubAfterRepair::run(OSD* osd,
			  OSDShard* sdata,
			  PGRef& pg,
			  ThreadPool::TPHandle& handle)
{
  pg->recovery_scrub(epoch_queued, handle);
  pg->unlock();
}

void PGScrubResched::run(OSD* osd,
			 OSDShard* sdata,
			 PGRef& pg,
			 ThreadPool::TPHandle& handle)
{
  pg->scrub_send_scrub_resched(epoch_queued, handle);
  pg->unlock();
}

void PGScrubPushesUpdate::run(OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_pushes_update(epoch_queued, handle);
  pg->unlock();
}

void PGScrubAppliedUpdate::run(OSD* osd,
			       OSDShard* sdata,
			       PGRef& pg,
			       ThreadPool::TPHandle& handle)
{
  pg->scrub_send_applied_update(epoch_queued, handle);
  pg->unlock();
}

void PGScrubUnblocked::run(OSD* osd,
			   OSDShard* sdata,
			   PGRef& pg,
			   ThreadPool::TPHandle& handle)
{
  pg->scrub_send_unblocking(epoch_queued, handle);
  pg->unlock();
}

void PGScrubDigestUpdate::run(OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_digest_update(epoch_queued, handle);
  pg->unlock();
}

void PGScrubGotReplMaps::run(OSD* osd,
			     OSDShard* sdata,
			     PGRef& pg,
			     ThreadPool::TPHandle& handle)
{
  pg->scrub_send_replmaps_ready(epoch_queued, handle);
  pg->unlock();
}

void PGRepScrub::run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle)
{
  pg->replica_scrub(epoch_queued, activation_index, handle);
  pg->unlock();
}

void PGRepScrubResched::run(OSD* osd,
			    OSDShard* sdata,
			    PGRef& pg,
			    ThreadPool::TPHandle& handle)
{
  pg->replica_scrub_resched(epoch_queued, activation_index, handle);
  pg->unlock();
}

void PGScrubReplicaPushes::run([[maybe_unused]] OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_replica_pushes(epoch_queued, handle);
  pg->unlock();
}

void PGScrubScrubFinished::run([[maybe_unused]] OSD* osd,
			       OSDShard* sdata,
			       PGRef& pg,
			       ThreadPool::TPHandle& handle)
{
  pg->scrub_send_scrub_is_finished(epoch_queued, handle);
  pg->unlock();
}

void PGScrubGetNextChunk::run([[maybe_unused]] OSD* osd,
			       OSDShard* sdata,
			       PGRef& pg,
			       ThreadPool::TPHandle& handle)
{
  pg->scrub_send_get_next_chunk(epoch_queued, handle);
  pg->unlock();
}

void PGScrubChunkIsBusy::run([[maybe_unused]] OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_chunk_busy(epoch_queued, handle);
  pg->unlock();
}

void PGScrubChunkIsFree::run([[maybe_unused]] OSD* osd,
			      OSDShard* sdata,
			      PGRef& pg,
			      ThreadPool::TPHandle& handle)
{
  pg->scrub_send_chunk_free(epoch_queued, handle);
  pg->unlock();
}

void PGRecovery::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->logger->tinc(
    l_osd_recovery_queue_lat,
    time_queued - ceph_clock_now());
  osd->do_recovery(pg.get(), epoch_queued, reserved_pushes, priority, handle);
  pg->unlock();
}

void PGRecoveryContext::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->logger->tinc(
    l_osd_recovery_context_queue_lat,
    time_queued - ceph_clock_now());
  c.release()->complete(handle);
  pg->unlock();
}

void PGDelete::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_delete(sdata, pg.get(), epoch_queued, handle);
}

void PGRecoveryMsg::run(
  OSD *osd,
  OSDShard *sdata,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  auto latency = time_queued - ceph_clock_now();
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    osd->logger->tinc(l_osd_recovery_push_queue_lat, latency);
  case MSG_OSD_PG_PUSH_REPLY:
    osd->logger->tinc(l_osd_recovery_push_reply_queue_lat, latency);
  case MSG_OSD_PG_PULL:
    osd->logger->tinc(l_osd_recovery_pull_queue_lat, latency);
  case MSG_OSD_PG_BACKFILL:
    osd->logger->tinc(l_osd_recovery_backfill_queue_lat, latency);
  case MSG_OSD_PG_BACKFILL_REMOVE:
    osd->logger->tinc(l_osd_recovery_backfill_remove_queue_lat, latency);
  case MSG_OSD_PG_SCAN:
    osd->logger->tinc(l_osd_recovery_scan_queue_lat, latency);
  }
  osd->dequeue_op(pg, op, handle);
  pg->unlock();
}

}
