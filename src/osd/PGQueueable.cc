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

void PGQueueable::DisplayVis::operator()(const OpRequestRef &op) {
  Message& msg = *boost::get<OpRequestRef>(op)->get_req();
  if (MSG_OSD_SUBOP == msg.get_header().type) {
    out << "osd-sub-op ";
  } else {
    out << "client-op, type:" << msg.get_type() <<
      ", tid:" << msg.get_tid() <<
      ", source_addr:" << msg.get_source_addr() <<
      ", source_inst:" << msg.get_source_inst() <<
      " ";
  }
}

void PGQueueable::DisplayVis::operator()(const PGSnapTrim &op) {
  out << "snaptrim ";
}

void PGQueueable::DisplayVis::operator()(const PGScrub &op) {
  out << "scrub ";
}

void PGQueueable::DisplayVis::operator()(const PGRecovery &op) {
  out << "recovery ";
}

std::ostream& operator<<(std::ostream& out, const PGQueueable& q) {
  PGQueueable::DisplayVis visitor(out);
  out << "req:{ owner:" << q.owner << " ";
  boost::apply_visitor(visitor, q.qvariant);
  out << "}";
  return out;
}
