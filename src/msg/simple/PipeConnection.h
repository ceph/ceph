// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_PIPECONNECTION_H
#define CEPH_MSG_PIPECONNECTION_H

#include "msg/Connection.h"

class Pipe;

class PipeConnection : public Connection {
  Pipe* pipe;

  friend class boost::intrusive_ptr<PipeConnection>;
  friend class Pipe;

public:
  int rx_buffers_version;
  map<ceph_tid_t,pair<bufferlist,int> > rx_buffers;

  PipeConnection(CephContext *cct, Messenger *m)
    : Connection(cct, m),
      pipe(NULL), rx_buffers_version(0) { }

  ~PipeConnection();

  Pipe* get_pipe();

  bool try_get_pipe(Pipe** p);

  bool clear_pipe(Pipe* old_p);

  void reset_pipe(Pipe* p);

  bool is_connected();

  int send_message(Message *m);
  void send_keepalive();
  void mark_down();
  void mark_disposable();
  void post_rx_buffer(ceph_tid_t tid, bufferlist& bl) {
    Mutex::Locker l(lock);
    ++rx_buffers_version;
    rx_buffers[tid] = pair<bufferlist,int>(bl, rx_buffers_version);
  }

  void revoke_rx_buffer(ceph_tid_t tid) {
    Mutex::Locker l(lock);
    rx_buffers.erase(tid);
  }
}; /* PipeConnection */

typedef boost::intrusive_ptr<PipeConnection> PipeConnectionRef;

#endif
