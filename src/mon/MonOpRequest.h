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

#include "common/TrackedOp.h"
#include "include/memory.h"
#include "mon/Session.h"
#include "msg/Message.h"

struct MonOpRequest : public TrackedOp {
  friend class OpTracker;

private:
  Message *request;
  utime_t dequeued_time;
  MonSession *session;
  ConnectionRef con;

  MonOpRequest(Message *req, OpTracker *tracker) :
    TrackedOp(tracker, req->get_recv_stamp()),
    request(req->get()),
    session(NULL),
    con(NULL)
  {
    tracker->mark_event(this, "header_read", request->get_recv_stamp());
    tracker->mark_event(this, "throttled", request->get_throttle_stamp());
    tracker->mark_event(this, "all_read", request->get_recv_complete_stamp());
    tracker->mark_event(this, "dispatched", request->get_dispatch_stamp());

    if (req) {
      con = req->get_connection();
      if (con) {
        session = static_cast<MonSession*>(con->get_priv());
      }
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

  MonSession *get_session() const {
    if (!session)
      return NULL;
    return (MonSession*)session->get();
  }

  template<class T>
  T *get_req() const { return static_cast<T*>(request); }

  Message *get_req() const { return get_req<Message>(); }

  ConnectionRef get_connection() { return con; }

  void set_session(MonSession *s) {
    if (session) {
      // we will be rewriting the existing session; drop the ref.
      session->put();
    }

    if (s == NULL) {
      session = NULL;
    } else {
      session = static_cast<MonSession*>(s->get());
    }
  }

  bool is_src_mon() const {
    return (con && con->get_peer_type() & CEPH_ENTITY_TYPE_MON);
  }

  typedef ceph::shared_ptr<MonOpRequest> Ref;

  void send_reply(Message *reply);
};

typedef MonOpRequest::Ref MonOpRequestRef;

#endif /* MON_OPREQUEST_H_ */
