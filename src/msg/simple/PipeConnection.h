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

  PipeConnection(CephContext *cct, Messenger *m)
    : Connection(cct, m),
      pipe(NULL) { }

  ~PipeConnection();

  Pipe* get_pipe();

  bool try_get_pipe(Pipe** p);

  bool clear_pipe(Pipe* old_p);

  void reset_pipe(Pipe* p);

  bool is_connected() override;

  int send_message(Message *m) override;
  void send_keepalive() override;
  void mark_down() override;
  void mark_disposable() override;

}; /* PipeConnection */

typedef boost::intrusive_ptr<PipeConnection> PipeConnectionRef;

#endif
