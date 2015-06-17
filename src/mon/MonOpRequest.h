// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat <contact@redhat.com>
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

#include "common/TrackedOp.h"
#include "include/memory.h"
#include "mon/Session.h"
#include "msg/Message.h"

struct MonOpRequest : public TrackedOp {
  friend class OpTracker;

  void mark_dispatch() {
    mark_event("monitor_dispatch");
  }
  void mark_wait_for_quorum() {
    mark_event("wait_for_quorum");
  }
  void mark_zap() {
    mark_event("monitor_zap");
  }

  void mark_svc_event(const string &service, const string &event) {
    string s = service;
    s.append(":").append(event);
    mark_event(s);
  }

  void mark_logmon_event(const string &event) {
    mark_svc_event("logm", event);
  }
  void mark_osdmon_event(const string &event) {
    mark_svc_event("osdmap", event);
  }
  void mark_pgmon_event(const string &event) {
    mark_svc_event("pgmap", event);
  }
  void mark_mdsmon_event(const string &event) {
    mark_svc_event("mdsmap", event);
  }
  void mark_authmon_event(const string &event) {
    mark_svc_event("auth", event);
  }
  void mark_paxos_event(const string &event) {
    mark_svc_event("paxos", event);
  }

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

  void _dump(utime_t now, Formatter *f) const {
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
      f->dump_int("seq", seq);
      f->dump_bool("src_is_mon", is_src_mon());
      f->dump_stream("source") << request->get_source_inst();
      f->close_section();
    }
  }

};

typedef MonOpRequest::Ref MonOpRequestRef;

#endif /* MON_OPREQUEST_H_ */
