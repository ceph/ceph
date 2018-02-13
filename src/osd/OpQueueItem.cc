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

#include "OpQueueItem.h"
#include "OSD.h"

void PGOpItem::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  osd->dequeue_op(pg, op, handle);
  pg->unlock();
}

void PGPeeringItem::run(
  OSD *osd,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_peering_evt(pg.get(), evt, handle);
}

void PGSnapTrim::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  pg->snap_trimmer(epoch_queued);
  pg->unlock();
}

void PGScrub::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  pg->scrub(epoch_queued, handle);
  pg->unlock();
}

void PGRecovery::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  osd->do_recovery(pg.get(), epoch_queued, reserved_pushes, handle);
  pg->unlock();
}

void PGDelete::run(
  OSD *osd,
  PGRef& pg,
  ThreadPool::TPHandle &handle)
{
  osd->dequeue_delete(pg.get(), epoch_queued, handle);
}

