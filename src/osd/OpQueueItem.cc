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
}

bool PGOpItem::needs_exclusive_pglock(OSD *osd)
{
  const MOSDOp *m = static_cast<const MOSDOp*>(op->get_req());
  // This is **buggy**. Sorry. We can't use op::may_write() and friends.
  // OSD::init_op_flags() hasn't been called yet and there is very good
  // reason for that: the MOSDop isn't fully parsed at this stage!
  // Maybe a bit assistance from a client would be needed. Will see.
  return !m->has_flag(CEPH_OSD_FLAG_READ) ||
          m->has_flag(CEPH_OSD_FLAG_WRITE);
}

void PGSnapTrim::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  pg->snap_trimmer(epoch_queued);
}

void PGScrub::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  pg->scrub(epoch_queued, handle);
}

void PGRecovery::run(OSD *osd,
                   PGRef& pg,
                   ThreadPool::TPHandle &handle)
{
  osd->do_recovery(pg.get(), epoch_queued, reserved_pushes, handle);
}


