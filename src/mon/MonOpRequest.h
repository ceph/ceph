// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc. <contact@redhat.com>
 * Copyright (C) 2015 SUSE LINUX GmbH
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

  int rw_flags;

  typedef enum {
    OP_TYPE_NONE,
    OP_TYPE_COMMAND,
    OP_TYPE_SERVICE,
    OP_TYPE_ELECTION,
    OP_TYPE_PROBE,
    OP_TYPE_SYNC,
    OP_TYPE_SCRUB,
  } op_type_t;

  void set_write();
  void set_read();

  op_type_t op_type;

  void set_op_type(op_type_t t) {
    op_type = t;
  }

  void set_command_op() {
    set_op_type(OP_TYPE_COMMAND);
  }
  void set_service_op() {
    set_op_type(OP_TYPE_SERVICE);
  }
  void set_election_op() {
    set_op_type(OP_TYPE_ELECTION);
  }
  void set_probe_op() {
    set_op_type(OP_TYPE_PROBE);
  }
  void set_sync_op() {
    set_op_type(OP_TYPE_SYNC);
  }
  void set_scrub_op() {
    set_op_type(OP_TYPE_SCRUB);
  }

  void mark_dispatch() {
    mark_event("monitor_dispatch");
  }
  void mark_wait_for_quorum() {
    mark_event("wait_for_quorum");
  }
  void mark_zap() {
    mark_event("monitor_zap");
  }

  void set_forwarded_msg() {
    is_forwarded_msg = true;
  }

  const string get_op_type_name() const {
    switch (op_type) {
      case OP_TYPE_NONE:
        return "none";
      case OP_TYPE_COMMAND:
        return "command";
      case OP_TYPE_SERVICE:
        return "service";
      case OP_TYPE_ELECTION:
        return "election";
      case OP_TYPE_PROBE:
        return "probe";
      case OP_TYPE_SYNC:
        return "sync";
      case OP_TYPE_SCRUB:
        return "scrub";
      default:
        assert(0 == "unknown op type");
    }
  }

private:
  Message *request;
  utime_t dequeued_time;
  MonSession *session;
  ConnectionRef connection;

  bool is_forwarded_msg;

  MonOpRequest(Message *req, OpTracker *tracker) :
    TrackedOp(tracker, req->get_recv_stamp()),
    op_type(OP_TYPE_NONE),
    request(req->get()),
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

  bool is_src_mon() const {
    return (connection && connection->get_peer_type() & CEPH_ENTITY_TYPE_MON);
  }

  typedef ceph::shared_ptr<MonOpRequest> Ref;

  void send_reply(Message *reply);

  void _dump(utime_t now, Formatter *f) const {
    Message *m = request;
    {
      f->open_array_section("events");
      Mutex::Locker l(lock);
      for (list<pair<utime_t,string> >::const_iterator i = events.begin();
           i != events.end(); ++i) {
        f->open_object_section("event");
        f->dump_stream("time") << i->first;
        f->dump_string("event", i->second);
        f->close_section();
      }
      f->close_section();
      f->open_object_section("info");
      f->dump_bool("forwarded", is_forwarded_msg);
      f->dump_bool("src_is_mon", is_src_mon());
      f->dump_string("op_type", get_op_type_name());
      f->close_section();
    }
  }

};

typedef MonOpRequest::Ref MonOpRequestRef;

#endif /* MON_OPREQUEST_H_ */
