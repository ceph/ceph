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

  MonOpRequest(const MonOpRequest &other) = delete;
  MonOpRequest & operator = (const MonOpRequest &other) = delete;

private:
  Message *request;
  utime_t dequeued_time;
  RefCountedPtr session;
  ConnectionRef con;
  bool forwarded_to_leader;
  op_type_t op_type;

  MonOpRequest(Message *req, OpTracker *tracker) :
    TrackedOp(tracker,
      req->get_recv_stamp().is_zero() ?
      ceph_clock_now() : req->get_recv_stamp()),
    request(req),
    con(NULL),
    forwarded_to_leader(false),
    op_type(OP_TYPE_NONE)
  {
    if (req) {
      con = req->get_connection();
      if (con) {
        session = con->get_priv();
      }
    }
  }

  void _dump(Formatter *f) const override {
    {
      f->open_array_section("events");
      std::lock_guard l(lock);
      for (auto& i : events) {
	f->dump_object("event", i);
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
  void _dump_op_descriptor_unlocked(ostream& stream) const override {
    get_req()->print(stream);
  }

public:
  ~MonOpRequest() override {
    request->put();
  }

  MonSession *get_session() const {
    return static_cast<MonSession*>(session.get());
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
    session.reset(s);
  }

  bool is_src_mon() const {
    return (con && con->get_peer_type() & CEPH_ENTITY_TYPE_MON);
  }

  typedef boost::intrusive_ptr<MonOpRequest> Ref;

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

struct C_MonOp : public Context
{
  MonOpRequestRef op;

  explicit C_MonOp(MonOpRequestRef o) :
    op(o) { }

  void finish(int r) override {
    if (op && r == -ECANCELED) {
      op->mark_event("callback canceled");
    } else if (op && r == -EAGAIN) {
      op->mark_event("callback retry");
    } else if (op && r == 0) {
      op->mark_event("callback finished");
    }
    _finish(r);
  }

  void mark_op_event(const string &event) {
    if (op)
      op->mark_event(event);
  }

  virtual void _finish(int r) = 0;
};

#endif /* MON_OPREQUEST_H_ */
