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

#include "osd/osd_op_util.h"
#include "osd/osd_types.h"
#include "common/TrackedOp.h"
#include "common/tracer.h"
/**
 * The OpRequest takes in a Message* and takes over a single reference
 * to it, which it puts() when destroyed.
 */
struct OpRequest : public TrackedOp {
  friend class OpTracker;

private:
  OpInfo op_info;

public:
  int maybe_init_op_info(const OSDMap &osdmap);

  auto get_flags() const { return op_info.get_flags(); }
  bool op_info_needs_init() const { return op_info.get_flags() == 0; }
  bool check_rmw(int flag) const { return op_info.check_rmw(flag); }
  bool may_read() const { return op_info.may_read(); }
  bool may_read_data() const { return op_info.may_read_data(); }
  bool may_write() const { return op_info.may_write(); }
  bool may_cache() const { return op_info.may_cache(); }
  bool rwordered_forced() const { return op_info.rwordered_forced(); }
  bool rwordered() const { return op_info.rwordered(); }
  bool includes_pg_op() const { return op_info.includes_pg_op(); }
  bool need_read_cap() const { return op_info.need_read_cap(); }
  bool need_write_cap() const { return op_info.need_write_cap(); }
  bool need_promote() const { return op_info.need_promote(); }
  bool need_skip_handle_cache() const { return op_info.need_skip_handle_cache(); }
  bool need_skip_promote() const { return op_info.need_skip_promote(); }
  bool allows_returnvec() const { return op_info.allows_returnvec(); }

  std::vector<OpInfo::ClassInfo> classes() const {
    return op_info.get_classes();
  }

  void _dump(ceph::Formatter *f) const override;

  bool has_feature(uint64_t f) const {
    return request->get_connection()->has_feature(f);
  }

private:
  Message *request; /// the logical request we are tracking
  osd_reqid_t reqid;
  entity_inst_t req_src_inst;
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  const char* last_event_detail = nullptr;
  utime_t dequeued_time;
  static const uint8_t flag_queued_for_pg=1 << 0;
  static const uint8_t flag_reached_pg =  1 << 1;
  static const uint8_t flag_delayed =     1 << 2;
  static const uint8_t flag_started =     1 << 3;
  static const uint8_t flag_sub_op_sent = 1 << 4;
  static const uint8_t flag_commit_sent = 1 << 5;

  OpRequest(Message *req, OpTracker *tracker);

protected:
  void _dump_op_descriptor(std::ostream& stream) const override;
  void _unregistered() override;
  bool filter_out(const std::set<std::string>& filters) override;

public:
  ~OpRequest() override {
    request->put();
  }

  bool check_send_map = true; ///< true until we check if sender needs a map
  epoch_t sent_epoch = 0;     ///< client's map epoch
  epoch_t min_epoch = 0;      ///< min epoch needed to handle this msg

  bool hitset_inserted;
  jspan osd_parent_span;

  template<class T>
  const T* get_req() const { return static_cast<const T*>(request); }

  const Message *get_req() const { return request; }
  Message *get_nonconst_req() { return request; }

  entity_name_t get_source() {
    if (request) {
      return request->get_source();
    } else {
      return entity_name_t();
    }
  }
  uint8_t state_flag() const {
    return latest_flag_point;
  }

  std::string _get_state_string() const override {
    switch(latest_flag_point) {
    case flag_queued_for_pg: return "queued for pg";
    case flag_reached_pg: return "reached pg";
    case flag_delayed: return last_event_detail;
    case flag_started: return "started";
    case flag_sub_op_sent: return "waiting for sub ops";
    case flag_commit_sent: return "commit sent; apply or cleanup";
    default: break;
    }
    return "no flag points reached";
  }

  static std::string get_state_string(uint8_t flag) {
    std::string flag_point;

    switch(flag) {
      case flag_queued_for_pg:
        flag_point = "queued for pg";
        break;
      case flag_reached_pg:
        flag_point = "reached pg";
        break;
      case flag_delayed:
        flag_point = "delayed";
        break;
      case flag_started:
        flag_point = "started";
        break;
      case flag_sub_op_sent:
        flag_point = "waiting for sub ops";
        break;
      case flag_commit_sent:
        flag_point = "commit sent; apply or cleanup";
        break;
    }
    return flag_point;
  }

  void mark_queued_for_pg() {
    mark_flag_point(flag_queued_for_pg, "queued_for_pg");
  }
  void mark_reached_pg() {
    mark_flag_point(flag_reached_pg, "reached_pg");
  }
  void mark_delayed(const char* s) {
    mark_flag_point(flag_delayed, s);
  }
  void mark_started() {
    mark_flag_point(flag_started, "started");
  }
  void mark_sub_op_sent(const std::string& s) {
    mark_flag_point_string(flag_sub_op_sent, s);
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

  typedef boost::intrusive_ptr<OpRequest> Ref;

private:
  void mark_flag_point(uint8_t flag, const char *s);
  void mark_flag_point_string(uint8_t flag, const std::string& s);
};

typedef OpRequest::Ref OpRequestRef;

#endif /* OPREQUEST_H_ */
