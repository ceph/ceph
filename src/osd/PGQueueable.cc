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


#include "PG.h"
#include "PGQueueable.h"
#include "OSD.h"


void PGQueueable::RunVis::operator()(const OpRequestRef &op) {
  osd->dequeue_op(pg, op, handle);
}

void PGQueueable::RunVis::operator()(const PGSnapTrim &op) {
  pg->snap_trimmer(op.epoch_queued);
}

void PGQueueable::RunVis::operator()(const PGScrub &op) {
  pg->scrub(op.epoch_queued, handle);
}

void PGQueueable::RunVis::operator()(const PGRecovery &op) {
  osd->do_recovery(pg.get(), op.epoch_queued, op.reserved_pushes, handle);
}
