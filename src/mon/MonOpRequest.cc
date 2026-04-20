// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "mon/MonOpRequest.h"
#include "mon/Session.h"
#include "common/Formatter.h"

void MonOpRequest::mark_svc_event(const std::string &service, const std::string &event) {
  std::string s = service;
  s.append(":").append(event);
  mark_event(s);
}

MonOpRequest::MonOpRequest(Message *req, OpTracker *tracker) :
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

void MonOpRequest::_dump(ceph::Formatter *f) const {
  {
    f->open_array_section("events");
    std::lock_guard l(lock);
    for (auto i = events.begin(); i != events.end(); ++i) {
      f->open_object_section("event");
      f->dump_string("event", i->str);
      f->dump_stream("time") << i->stamp;

      auto i_next = i + 1;

      if (i_next < events.end()) {
	f->dump_float("duration", i_next->stamp - i->stamp);
      } else {
	f->dump_float("duration", events.rbegin()->stamp - get_initiated());
      }

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

void MonOpRequest::_dump_op_descriptor(std::ostream& stream) const {
  get_req()->print(stream);
}

MonOpRequest::~MonOpRequest() {
  request->put();
}

MonSession *MonOpRequest::get_session() const {
  return static_cast<MonSession*>(session.get());
}

void MonOpRequest::set_session(MonSession *s) {
  session.reset(s);
}

void C_MonOp::finish(int r) {
  if (op && r == -ECANCELED) {
    op->mark_event("callback canceled");
  } else if (op && r == -EAGAIN) {
    op->mark_event("callback retry");
  } else if (op && r == 0) {
    op->mark_event("callback finished");
  }
  _finish(r);
}
