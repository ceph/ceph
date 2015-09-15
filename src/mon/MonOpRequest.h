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
#include <iosfwd>
#include <stdint.h>

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
  void mark_forwarded() {
    mark_event("forwarded");
    forwarded_to_leader = true;
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


  enum op_type_t {
    OP_TYPE_NONE    = 0,      ///< no type defined (default)
    OP_TYPE_SERVICE,          ///< belongs to a Paxos Service or similar
    OP_TYPE_MONITOR,          ///< belongs to the Monitor class
    OP_TYPE_ELECTION,         ///< belongs to the Elector class
    OP_TYPE_PAXOS,            ///< refers to Paxos messages
    OP_TYPE_COMMAND,          ///< is a command
  };

private:
  Message *request;
  utime_t dequeued_time;
  MonSession *session;
  ConnectionRef con;
  bool forwarded_to_leader;
  op_type_t op_type;

  MonOpRequest(Message *req, OpTracker *tracker) :
    TrackedOp(tracker, req->get_recv_stamp()),
    request(req),
    session(NULL),
    con(NULL),
    forwarded_to_leader(false),
    op_type(OP_TYPE_NONE)
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
      f->dump_bool("forwarded_to_leader", forwarded_to_leader);
      f->close_section();
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
    return session;
  }

  template<class T>
  T *get_req() const { return static_cast<T*>(request); }

  Message *get_req() const { return get_req<Message>(); }

  int get_req_type() const {
    if (!request)
      return 0;
    return request->get_type();
  }

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

  void set_op_type(op_type_t t) {
    op_type = t;
  }
  void set_type_service() {
    set_op_type(OP_TYPE_SERVICE);
  }
  void set_type_monitor() {
    set_op_type(OP_TYPE_MONITOR);
  }
  void set_type_paxos() {
    set_op_type(OP_TYPE_PAXOS);
  }
  void set_type_election() {
    set_op_type(OP_TYPE_ELECTION);
  }
  void set_type_command() {
    set_op_type(OP_TYPE_COMMAND);
  }

  op_type_t get_op_type() {
    return op_type;
  }

  bool is_type_service() {
    return (get_op_type() == OP_TYPE_SERVICE);
  }
  bool is_type_monitor() {
    return (get_op_type() == OP_TYPE_MONITOR);
  }
  bool is_type_paxos() {
    return (get_op_type() == OP_TYPE_PAXOS);
  }
  bool is_type_election() {
    return (get_op_type() == OP_TYPE_ELECTION);
  }
  bool is_type_command() {
    return (get_op_type() == OP_TYPE_COMMAND);
  }
};

typedef MonOpRequest::Ref MonOpRequestRef;

#endif /* MON_OPREQUEST_H_ */
