// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef OPREQUEST_H_
#define OPREQUEST_H_
#include <sstream>
#include <stdint.h>
#include <vector>

#include <include/utime.h>
#include "common/Mutex.h"
#include "include/xlist.h"
#include "msg/Message.h"
#include "include/memory.h"
#include "common/TrackedOp.h"

/**
 * osd request identifier
 *
 * caller name + incarnation# + tid to unique identify this request.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  ceph_tid_t         tid;
  int32_t       inc;  // incarnation

  osd_reqid_t()
    : tid(0), inc(0) {}
  osd_reqid_t(const entity_name_t& a, int i, ceph_tid_t t)
    : name(a), tid(t), inc(i) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<osd_reqid_t*>& o);
};
WRITE_CLASS_ENCODER(osd_reqid_t)

/**
 * The OpRequest takes in a Message* and takes over a single reference
 * to it, which it puts() when destroyed.
 */
struct OpRequest : public TrackedOp {
  friend class OpTracker;

  // rmw flags
  int rmw_flags;

  bool check_rmw(int flag);
  bool may_read();
  bool may_write();
  bool may_cache();
  bool includes_pg_op();
  bool need_read_cap();
  bool need_write_cap();
  bool need_class_read_cap();
  bool need_class_write_cap();
  bool need_promote();
  bool need_skip_handle_cache();
  bool need_skip_promote();
  void set_read();
  void set_write();
  void set_cache();
  void set_class_read();
  void set_class_write();
  void set_pg_op();
  void set_promote();
  void set_skip_handle_cache();
  void set_skip_promote();

  void _dump(utime_t now, Formatter *f) const;

  bool has_feature(uint64_t f) const {
    return request->get_connection()->has_feature(f);
  }

private:
  Message *request; /// the logical request we are tracking
  osd_reqid_t reqid;
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  utime_t dequeued_time;
  static const uint8_t flag_queued_for_pg=1 << 0;
  static const uint8_t flag_reached_pg =  1 << 1;
  static const uint8_t flag_delayed =     1 << 2;
  static const uint8_t flag_started =     1 << 3;
  static const uint8_t flag_sub_op_sent = 1 << 4;
  static const uint8_t flag_commit_sent = 1 << 5;

  OpRequest(Message *req, OpTracker *tracker);

protected:
  void _dump_op_descriptor_unlocked(ostream& stream) const;
  void _unregistered();

public:
  ~OpRequest() {
    request->put();
  }
  bool send_map_update;
  epoch_t sent_epoch;
  bool hitset_inserted;
  Message *get_req() const { return request; }
  bool been_queued_for_pg() { return hit_flag_points & flag_queued_for_pg; }
  bool been_reached_pg() { return hit_flag_points & flag_reached_pg; }
  bool been_delayed() { return hit_flag_points & flag_delayed; }
  bool been_started() { return hit_flag_points & flag_started; }
  bool been_sub_op_sent() { return hit_flag_points & flag_sub_op_sent; }
  bool been_commit_sent() { return hit_flag_points & flag_commit_sent; }
  bool currently_queued_for_pg() { return latest_flag_point & flag_queued_for_pg; }
  bool currently_reached_pg() { return latest_flag_point & flag_reached_pg; }
  bool currently_delayed() { return latest_flag_point & flag_delayed; }
  bool currently_started() { return latest_flag_point & flag_started; }
  bool currently_sub_op_sent() { return latest_flag_point & flag_sub_op_sent; }
  bool currently_commit_sent() { return latest_flag_point & flag_commit_sent; }

  const char *state_string() const {
    switch(latest_flag_point) {
    case flag_queued_for_pg: return "queued for pg";
    case flag_reached_pg: return "reached pg";
    case flag_delayed: return "delayed";
    case flag_started: return "started";
    case flag_sub_op_sent: return "waiting for sub ops";
    case flag_commit_sent: return "commit sent; apply or cleanup";
    default: break;
    }
    return "no flag points reached";
  }

  void mark_queued_for_pg() {
    mark_flag_point(flag_queued_for_pg, "queued_for_pg");
  }
  void mark_reached_pg() {
    mark_flag_point(flag_reached_pg, "reached_pg");
  }
  void mark_delayed(const string& s) {
    mark_flag_point(flag_delayed, s);
  }
  void mark_started() {
    mark_flag_point(flag_started, "started");
  }
  void mark_sub_op_sent(const string& s) {
    mark_flag_point(flag_sub_op_sent, s);
  }
  void mark_commit_sent() {
    mark_flag_point(flag_commit_sent, "commit_sent");
  }

  utime_t get_dequeued_time() const {
    return dequeued_time;
  }
  void set_dequeued_time(utime_t deq_time) {
    dequeued_time = deq_time;
  }

  osd_reqid_t get_reqid() const {
    return reqid;
  }

  typedef ceph::shared_ptr<OpRequest> Ref;

private:
  void set_rmw_flags(int flags);
  void mark_flag_point(uint8_t flag, const string& s);
};

typedef OpRequest::Ref OpRequestRef;

#endif /* OPREQUEST_H_ */
