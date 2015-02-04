// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef MON_OPREQUEST_H_
#define MON_OPREQUEST_H_
#include <sstream>
#include <stdint.h>
#include <vector>

#include <include/utime.h>
#include "common/Mutex.h"
#include "include/xlist.h"
#include "msg/Message.h"
#include "include/memory.h"
#include "common/TrackedOp.h"
#include "mon/Session.h"

struct MonOpRequest : public TrackedOp {
  friend class OpTracker;

private:
  Message *request;
  utime_t dequeued_time;
  bool src_is_mon;
  MonSession *session;
  ConnectionRef connection;

  MonOpRequest(Message *req, OpTracker *tracker) :
    TrackedOp(tracker, req->get_recv_stamp()),
    request(req->get()),
    src_is_mon(false),
    session(NULL),
    connection(NULL)
  {
    tracker->mark_event(this, "header_read", request->get_recv_stamp());
    tracker->mark_event(this, "throttled", request->get_throttle_stamp());
    tracker->mark_event(this, "all_read", request->get_recv_complete_stamp());
    tracker->mark_event(this, "dispatched", request->get_dispatch_stamp());

    if (req) {
      connection = req->get_connection();
    }

    if (connection) {
      session = static_cast<MonSession*>(connection->get_priv());
    }
  }

protected:
  void _dump_op_descriptor_unlocked(ostream& stream) const {
    get_req()->print(stream);
  }

public:
  ~MonOpRequest() {
    request->put();
    // certain ops may not have a session (e.g., AUTH or PING)
    if (session)
      session->put();
  }

  MonSession *get_req_session() const {
    if (!session)
      return NULL;
    return (MonSession*)session->get();
  }

  template<class T>
  T *get_req() const { return static_cast<T*>(request); }

  Message *get_req() const { return get_req<Message>(); }

  ConnectionRef get_connection() { return connection; }

  void set_session(MonSession *s) {
    if (s == NULL) {
      session = NULL;
    } else {
      session = static_cast<MonSession*>(s->get());
    }
  }

  bool is_src_mon() {
    return (connection && connection->get_peer_type() & CEPH_ENTITY_TYPE_MON);
  }
  void set_src_is_mon(bool s) { src_is_mon = s; }

  typedef ceph::shared_ptr<MonOpRequest> Ref;
};

typedef MonOpRequest::Ref MonOpRequestRef;

#endif /* MON_OPREQUEST_H_ */
