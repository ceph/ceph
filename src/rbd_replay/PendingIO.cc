// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "PendingIO.hpp"
#include "rbd_replay_debug.hpp"


using namespace std;
using namespace rbd_replay;

extern "C"
void rbd_replay_pending_io_callback(librbd::completion_t cb, void *arg) {
  PendingIO *io = static_cast<PendingIO*>(arg);
  io->completed(cb);
}

PendingIO::PendingIO(action_id_t id,
		     ActionCtx &worker)
  : m_id(id),
    m_completion(new librbd::RBD::AioCompletion(this, rbd_replay_pending_io_callback)),
    m_worker(worker) {
    }

PendingIO::~PendingIO() {
  m_completion->release();
}

void PendingIO::completed(librbd::completion_t cb) {
  dout(ACTION_LEVEL) << "Completed pending IO #" << m_id << dendl;
  ssize_t r = m_completion->get_return_value();
  assertf(r >= 0, "id = %d, r = %d", m_id, r);
  m_worker.remove_pending(shared_from_this());
}
